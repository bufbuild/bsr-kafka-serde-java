// Copyright 2025 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buf.bsr.kafka;

import build.buf.bsr.kafka.gen.buf.registry.module.v1.GetFileDescriptorSetResponse;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.tls.HandshakeCertificates;
import okhttp3.tls.HeldCertificate;
import okio.Buffer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

class ClientTest {

  @Test
  void testGetMessageDescriptor() throws Exception {
    Path resourcePath = Paths.get("src/test/resources/email-updated-fds.binpb");
    DescriptorProtos.FileDescriptorSet savedFds;
    try (FileInputStream fis = new FileInputStream(resourcePath.toFile())) {
      savedFds = DescriptorProtos.FileDescriptorSet.parseFrom(fis);
    }

    HeldCertificate serverCertificate = createSelfSignedCertificate();
    HandshakeCertificates certificates =
        new HandshakeCertificates.Builder()
            .heldCertificate(serverCertificate)
            .addTrustedCertificate(serverCertificate.certificate())
            .build();

    try (MockWebServer server = new MockWebServer()) {
      server.useHttps(certificates.sslSocketFactory(), false);

      Buffer buffer = new Buffer();
      GetFileDescriptorSetResponse response =
          GetFileDescriptorSetResponse.newBuilder().setFileDescriptorSet(savedFds).build();
      buffer.write(response.toByteArray());
      server.enqueue(new MockResponse().setBody(buffer));
      server.start();

      String host = server.getHostName() + ":" + server.getPort();
      Client client = new Client(host, null, certificates.sslContext());

      String messageFQN = "bufstream.demo.v1.EmailUpdated";
      Descriptors.Descriptor messageDescriptor =
          client.getMessageDescriptor("5c792fd712d44915acba9b1b37d33c87", messageFQN);
      Assertions.assertThat(messageDescriptor).isNotNull();
      Assertions.assertThat(messageDescriptor.getFullName()).isEqualTo(messageFQN);

      RecordedRequest request = server.takeRequest();
      Assertions.assertThat(request.getMethod()).isEqualTo("POST");
      Assertions.assertThat(request.getPath())
          .isEqualTo("/buf.registry.module.v1.FileDescriptorSetService/GetFileDescriptorSet");
      Assertions.assertThat(request.getHeader("Content-Type")).isEqualTo("application/proto");
      Assertions.assertThat(request.getHeader("Connect-Protocol-Version")).isEqualTo("1");
      Assertions.assertThat(request.getHeader("Connect-Timeout-Ms")).isEqualTo("30000");
      Assertions.assertThat(request.getHeader("Accept-Encoding")).isEqualTo("identity");
      Assertions.assertThat(request.getHeader("Authorization")).isNull();
    }
  }

  @Test
  void testGetMessageDescriptorNegativeCache() throws IOException {
    HeldCertificate serverCertificate = createSelfSignedCertificate();
    HandshakeCertificates certificates =
        new HandshakeCertificates.Builder()
            .heldCertificate(serverCertificate)
            .addTrustedCertificate(serverCertificate.certificate())
            .build();

    try (MockWebServer server = new MockWebServer()) {
      server.useHttps(certificates.sslSocketFactory(), false);

      int numThreads = 100;
      server.enqueue(new MockResponse().setResponseCode(404));
      for (int i = 1; i < numThreads; i++) {
        // Enqueue up to the maximum number of calls. We only expect one call to be made due to
        // negative caching.
        server.enqueue(new MockResponse().setResponseCode(500));
      }
      server.start();

      String host = server.getHostName() + ":" + server.getPort();
      TestClock clock = new TestClock();
      Client client = new Client(host, null, certificates.sslContext(), clock);

      String commitID = UUID.randomUUID().toString().replace("-", "");
      String messageFQN = "bufstream.demo.v1.EmailUpdated";
      ExecutorService executor = Executors.newFixedThreadPool(numThreads);
      try {
        CountDownLatch latch = new CountDownLatch(numThreads);
        List<Future<Descriptors.Descriptor>> futures = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
          futures.add(
              executor.submit(
                  () -> {
                    latch.countDown();
                    try {
                      latch.await();
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      throw new RuntimeException(e);
                    }
                    return client.getMessageDescriptor(commitID, messageFQN);
                  }));
        }
        for (Future<Descriptors.Descriptor> future : futures) {
          Assertions.assertThatThrownBy(future::get)
              .hasRootCauseInstanceOf(ClientException.class)
              .hasMessageContaining("HTTP 404");
        }

        // Negative caching should have only allowed a single request to go through.
        Assertions.assertThat(server.getRequestCount()).isEqualTo(1);

        // Update clock to right before negative cache expires - should still return 404.
        clock.plus(Client.NEGATIVE_CACHE_EXPIRY);
        Assertions.assertThatThrownBy(() -> client.getMessageDescriptor(commitID, messageFQN))
            .isInstanceOf(ClientException.class)
            .hasMessageContaining("HTTP 404");
        Assertions.assertThat(server.getRequestCount()).isEqualTo(1);

        // Make the cache entry expired.
        clock.plus(Duration.ofNanos(1));
        futures.clear();
        for (int i = 0; i < numThreads; i++) {
          futures.add(executor.submit(() -> client.getMessageDescriptor(commitID, messageFQN)));
        }
        for (Future<Descriptors.Descriptor> future : futures) {
          Assertions.assertThatThrownBy(future::get)
              .hasRootCauseInstanceOf(ClientException.class)
              .hasMessageContaining("HTTP 500");
        }
        // After expiration only a single additional call should've gone through.
        Assertions.assertThat(server.getRequestCount()).isEqualTo(2);
      } finally {
        executor.shutdown();
      }
    }
  }

  @Test
  @EnabledIfSystemProperty(named = "bsr.integration", matches = "true")
  void testGetMessageDescriptorAgainstDemoBufDev() {
    Client client = new Client("demo.buf.dev", null);
    String messageFQN = "bufstream.demo.v1.EmailUpdated";
    Descriptors.Descriptor messageDescriptor =
        client.getMessageDescriptor("5c792fd712d44915acba9b1b37d33c87", messageFQN);
    Assertions.assertThat(messageDescriptor).isNotNull();
    Assertions.assertThat(messageDescriptor.getFullName()).isEqualTo(messageFQN);
  }

  private static HeldCertificate createSelfSignedCertificate() {
    return new HeldCertificate.Builder()
        .addSubjectAlternativeName("localhost")
        .commonName("localhost")
        .build();
  }

  private static class TestClock extends Clock {

    private final AtomicReference<Instant> instant = new AtomicReference<>(Instant.now());

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Instant instant() {
      return instant.get();
    }

    public void plus(Duration duration) {
      this.instant.set(this.instant.get().plus(duration));
    }
  }
}
