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
        server.enqueue(new MockResponse().setResponseCode(400));
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
              .hasMessageContaining("HTTP 400");
        }
        // After expiration only a single additional call should've gone through.
        Assertions.assertThat(server.getRequestCount()).isEqualTo(2);
      } finally {
        executor.shutdown();
      }
    }
  }

  @Test
  void testRetryOnRetryableStatusCodes() throws Exception {
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

      // First request returns 503, second returns 429, third returns 200
      server.enqueue(new MockResponse().setResponseCode(503).setBody("Service Unavailable"));
      server.enqueue(new MockResponse().setResponseCode(429).setBody("Too Many Requests"));

      Buffer buffer = new Buffer();
      GetFileDescriptorSetResponse response =
          GetFileDescriptorSetResponse.newBuilder().setFileDescriptorSet(savedFds).build();
      buffer.write(response.toByteArray());
      server.enqueue(new MockResponse().setBody(buffer));

      server.start();

      String host = server.getHostName() + ":" + server.getPort();
      Client client = new Client(host, null, certificates.sslContext());

      String messageFQN = "bufstream.demo.v1.EmailUpdated";
      long startTime = System.currentTimeMillis();
      Descriptors.Descriptor messageDescriptor =
          client.getMessageDescriptor("5c792fd712d44915acba9b1b37d33c87", messageFQN);
      long elapsed = System.currentTimeMillis() - startTime;

      Assertions.assertThat(messageDescriptor).isNotNull();
      Assertions.assertThat(messageDescriptor.getFullName()).isEqualTo(messageFQN);

      // Verify we made 3 requests (2 failed, 1 successful)
      Assertions.assertThat(server.getRequestCount()).isEqualTo(3);

      // Verify exponential backoff was applied (1s + 2s = 3s minimum)
      Assertions.assertThat(elapsed).isGreaterThanOrEqualTo(3000);
    }
  }

  @Test
  void testRetryOn502BadGateway() throws Exception {
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

      // First request returns 502 Bad Gateway, second returns 200
      server.enqueue(new MockResponse().setResponseCode(502).setBody("Bad Gateway"));

      Buffer buffer = new Buffer();
      GetFileDescriptorSetResponse response =
          GetFileDescriptorSetResponse.newBuilder().setFileDescriptorSet(savedFds).build();
      buffer.write(response.toByteArray());
      server.enqueue(new MockResponse().setBody(buffer));

      server.start();

      String host = server.getHostName() + ":" + server.getPort();
      Client client = new Client(host, null, certificates.sslContext());

      String messageFQN = "bufstream.demo.v1.EmailUpdated";
      long startTime = System.currentTimeMillis();
      Descriptors.Descriptor messageDescriptor =
          client.getMessageDescriptor("5c792fd712d44915acba9b1b37d33c87", messageFQN);
      long elapsed = System.currentTimeMillis() - startTime;

      Assertions.assertThat(messageDescriptor).isNotNull();
      Assertions.assertThat(messageDescriptor.getFullName()).isEqualTo(messageFQN);

      // Verify we made 2 requests (1 failed with 502, 1 successful)
      Assertions.assertThat(server.getRequestCount()).isEqualTo(2);

      // Verify backoff was applied (at least 1s)
      Assertions.assertThat(elapsed).isGreaterThanOrEqualTo(1000);
    }
  }

  @Test
  void testRetryExhaustion() throws IOException {
    HeldCertificate serverCertificate = createSelfSignedCertificate();
    HandshakeCertificates certificates =
        new HandshakeCertificates.Builder()
            .heldCertificate(serverCertificate)
            .addTrustedCertificate(serverCertificate.certificate())
            .build();

    try (MockWebServer server = new MockWebServer()) {
      server.useHttps(certificates.sslSocketFactory(), false);

      // Return 503 repeatedly to exhaust retries
      for (int i = 0; i < 10; i++) {
        server.enqueue(new MockResponse().setResponseCode(503).setBody("Service Unavailable"));
      }
      server.start();

      String host = server.getHostName() + ":" + server.getPort();
      Client client = new Client(host, null, certificates.sslContext());

      String messageFQN = "bufstream.demo.v1.EmailUpdated";
      long startTime = System.currentTimeMillis();
      Assertions.assertThatThrownBy(
              () -> client.getMessageDescriptor("5c792fd712d44915acba9b1b37d33c87", messageFQN))
          .isInstanceOf(ClientException.class)
          .hasMessageContaining("HTTP 503");
      long elapsed = System.currentTimeMillis() - startTime;

      // Verify retries were attempted with exponential backoff up to 30 seconds total
      // Retries: 1s, 2s, 4s, 8s, 15s (remaining) = 30s total
      Assertions.assertThat(elapsed).isGreaterThanOrEqualTo(30000);

      // Verify multiple requests were made
      Assertions.assertThat(server.getRequestCount()).isGreaterThan(1);
    }
  }

  @Test
  void testNoRetryOnNonRetryableStatusCode() throws IOException {
    HeldCertificate serverCertificate = createSelfSignedCertificate();
    HandshakeCertificates certificates =
        new HandshakeCertificates.Builder()
            .heldCertificate(serverCertificate)
            .addTrustedCertificate(serverCertificate.certificate())
            .build();

    try (MockWebServer server = new MockWebServer()) {
      server.useHttps(certificates.sslSocketFactory(), false);

      // Return 404 which should not be retried
      server.enqueue(new MockResponse().setResponseCode(404).setBody("Not Found"));
      server.start();

      String host = server.getHostName() + ":" + server.getPort();
      Client client = new Client(host, null, certificates.sslContext());

      String messageFQN = "bufstream.demo.v1.EmailUpdated";
      long startTime = System.currentTimeMillis();
      Assertions.assertThatThrownBy(
              () -> client.getMessageDescriptor("5c792fd712d44915acba9b1b37d33c87", messageFQN))
          .isInstanceOf(ClientException.class)
          .hasMessageContaining("HTTP 404");
      long elapsed = System.currentTimeMillis() - startTime;

      // Should fail immediately without retrying
      Assertions.assertThat(elapsed).isLessThan(1000);

      // Verify only one request was made
      Assertions.assertThat(server.getRequestCount()).isEqualTo(1);
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
