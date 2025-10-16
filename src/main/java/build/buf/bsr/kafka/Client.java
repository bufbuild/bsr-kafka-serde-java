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

import build.buf.bsr.kafka.gen.buf.registry.module.v1.GetFileDescriptorSetRequest;
import build.buf.bsr.kafka.gen.buf.registry.module.v1.GetFileDescriptorSetResponse;
import build.buf.bsr.kafka.gen.buf.registry.module.v1.ResourceRef;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import javax.net.ssl.SSLContext;

final class Client implements BSRClient {

  static final String HEADER_AUTHORIZATION = "Authorization";
  static final String HEADER_CONTENT_TYPE = "Content-Type";
  static final String HEADER_CONNECT_PROTOCOL_VERSION = "Connect-Protocol-Version";
  static final String HEADER_CONNECT_TIMEOUT_MS = "Connect-Timeout-Ms";
  static final String HEADER_ACCEPT_ENCODING = "Accept-Encoding";
  static final String HEADER_USER_AGENT = "User-Agent";
  static final String BEARER_PREFIX = "Bearer ";
  static final String FILE_DESCRIPTOR_SET_SERVICE =
      "buf.registry.module.v1.FileDescriptorSetService";
  static final String METHOD_GET_FILE_DESCRIPTOR_SET =
      FILE_DESCRIPTOR_SET_SERVICE + "/GetFileDescriptorSet";

  private static final String USER_AGENT = buildUserAgent();
  private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(30);
  static final Duration NEGATIVE_CACHE_EXPIRY = Duration.ofMinutes(1);
  private static final Set<Integer> RETRYABLE_STATUS_CODES =
      Set.of(
          429, // Unavailable (also ResourceExhausted)
          502, // Unavailable
          503, // Unavailable
          504, // Unavailable
          500 // Internal
          );
  private static final long INITIAL_BACKOFF_MS = 1_000;
  private static final long MAX_TOTAL_BACKOFF_MS = 30_000;
  private static final long JITTER_MAX_MS = 100;

  private final String host;
  private final String token;
  private final Clock clock;
  private final HttpClient client;
  private final ConcurrentMap<CacheKey, CacheEntry> cache = new ConcurrentHashMap<>();

  Client(String host, String token) {
    this(host, token, null, Clock.systemUTC());
  }

  Client(String host, String token, SSLContext sslContext) {
    this(host, token, sslContext, Clock.systemUTC());
  }

  Client(String host, String token, SSLContext sslContext, Clock clock) {
    this.host = Objects.requireNonNull(host);
    this.token = token;
    this.clock = Objects.requireNonNull(clock);
    HttpClient.Builder builder = HttpClient.newBuilder();
    if (sslContext != null) {
      builder.sslContext(sslContext);
    }
    this.client = builder.build();
  }

  private static String buildUserAgent() {
    Package pkg = Client.class.getPackage();
    String title = pkg.getImplementationTitle();
    String version = pkg.getImplementationVersion();
    if (title != null && version != null) {
      return title + "/" + version;
    }
    return "bsr-kafka-serde/unknown";
  }

  private HttpRequest.Builder newRequestBuilder() {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder()
            .header(HEADER_CONTENT_TYPE, "application/proto")
            .header(HEADER_CONNECT_PROTOCOL_VERSION, "1")
            .header(HEADER_ACCEPT_ENCODING, "identity")
            .header(HEADER_USER_AGENT, USER_AGENT);
    if (token != null && !token.isEmpty()) {
      builder.header(HEADER_AUTHORIZATION, BEARER_PREFIX + token);
    }
    return builder;
  }

  @Override
  public Descriptors.Descriptor getMessageDescriptor(String commitID, String messageFQN)
      throws ClientException {
    CacheKey key = new CacheKey(commitID, messageFQN);
    CacheEntry cached = cache.get(key);
    while (cached != null) {
      if (!cached.future.isCompletedExceptionally()) {
        // We're either making a call or the call succeeded. Join the result.
        return cached.join();
      }
      // Call has failed - check if we should expire this cache entry.
      if (!cached.createdAt.plus(NEGATIVE_CACHE_EXPIRY).isBefore(this.clock.instant())) {
        return cached.join();
      }
      cache.remove(key, cached);
      cached = cache.get(key);
    }
    final CacheEntry entry = new CacheEntry(clock.instant());
    if ((cached = cache.putIfAbsent(key, entry)) != null) {
      // Lost the race to add the cache entry. Join the current call.
      return cached.join();
    }
    try {
      Descriptors.Descriptor descriptor = lookupDescriptorFromBSR(commitID, messageFQN);
      entry.future.complete(descriptor);
      return descriptor;
    } catch (ClientException e) {
      entry.future.completeExceptionally(e);
      throw e;
    } catch (Throwable t) {
      ClientException clientException = new ClientException(t);
      entry.future.completeExceptionally(clientException);
      throw clientException;
    }
  }

  private Descriptors.Descriptor lookupDescriptorFromBSR(String commitID, String messageFQN) {
    GetFileDescriptorSetRequest request =
        GetFileDescriptorSetRequest.newBuilder()
            .setResourceRef(ResourceRef.newBuilder().setId(commitID).build())
            .addIncludeTypes(messageFQN)
            .build();
    HttpRequest httpRequest =
        newRequestBuilder()
            .uri(URI.create("https://" + host + "/" + METHOD_GET_FILE_DESCRIPTOR_SET))
            .header(HEADER_CONNECT_TIMEOUT_MS, "30000")
            .timeout(DEFAULT_REQUEST_TIMEOUT)
            .POST(HttpRequest.BodyPublishers.ofByteArray(request.toByteArray()))
            .build();

    long totalBackoffMs = 0;
    long currentBackoffMs = INITIAL_BACKOFF_MS;

    while (true) {
      try {
        HttpResponse<byte[]> response =
            client.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() != 200) {
          String responseBody = new String(response.body(), StandardCharsets.UTF_8);

          // Check if we should retry on this status code
          boolean shouldRetry = RETRYABLE_STATUS_CODES.contains(response.statusCode());

          if (shouldRetry && totalBackoffMs < MAX_TOTAL_BACKOFF_MS) {
            // Calculate backoff with jitter to prevent thundering herd
            long remainingBackoffMs = MAX_TOTAL_BACKOFF_MS - totalBackoffMs;
            long jitterMs = ThreadLocalRandom.current().nextLong(JITTER_MAX_MS);
            long backoffMs = Math.min(currentBackoffMs + jitterMs, remainingBackoffMs);

            try {
              Thread.sleep(backoffMs);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              throw new ClientException(
                  String.format("%s failed", METHOD_GET_FILE_DESCRIPTOR_SET), ie);
            }

            totalBackoffMs += backoffMs;
            currentBackoffMs *= 2; // Exponential backoff
          } else {
            throw new ClientException(
                String.format(
                    "%s failed: HTTP %d - %s",
                    METHOD_GET_FILE_DESCRIPTOR_SET, response.statusCode(), responseBody));
          }
        } else {
          GetFileDescriptorSetResponse bsrResponse =
              GetFileDescriptorSetResponse.parseFrom(response.body());
          Descriptors.Descriptor descriptor =
              findMessageDescriptor(bsrResponse.getFileDescriptorSet(), messageFQN);
          if (descriptor == null) {
            throw new ClientException("failed to lookup message descriptor for " + messageFQN);
          }
          return descriptor;
        }
      } catch (IOException e) {
        throw new ClientException(String.format("%s failed", METHOD_GET_FILE_DESCRIPTOR_SET), e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ClientException(String.format("%s failed", METHOD_GET_FILE_DESCRIPTOR_SET), e);
      }
    }
  }

  static Descriptors.Descriptor findMessageDescriptor(
      DescriptorProtos.FileDescriptorSet fds, String messageFQN) {
    Map<String, Descriptors.FileDescriptor> descriptorsByName = new HashMap<>(fds.getFileCount());
    for (DescriptorProtos.FileDescriptorProto fd : fds.getFileList()) {
      try {
        buildFileDescriptor(fd, fds, descriptorsByName);
      } catch (Descriptors.DescriptorValidationException e) {
        throw new ClientException("failed to build file descriptor for " + fd.getName(), e);
      }
    }
    final String packageName, messageName;
    int lastDot = messageFQN.lastIndexOf('.');
    if (lastDot != -1) {
      packageName = messageFQN.substring(0, lastDot);
      messageName = messageFQN.substring(lastDot + 1);
    } else {
      packageName = "";
      messageName = messageFQN;
    }
    for (Descriptors.FileDescriptor fd : descriptorsByName.values()) {
      if (!fd.getPackage().equals(packageName)) {
        continue;
      }
      Descriptors.Descriptor md = fd.findMessageTypeByName(messageName);
      if (md != null) {
        return md;
      }
    }
    return null;
  }

  static Descriptors.FileDescriptor buildFileDescriptor(
      DescriptorProtos.FileDescriptorProto fdp,
      DescriptorProtos.FileDescriptorSet fds,
      Map<String, Descriptors.FileDescriptor> fileDescriptorsByName)
      throws Descriptors.DescriptorValidationException {
    if (fileDescriptorsByName.containsKey(fdp.getName())) {
      return fileDescriptorsByName.get(fdp.getName());
    }
    List<Descriptors.FileDescriptor> dependencies = new ArrayList<>(fdp.getDependencyCount());
    for (String depName : fdp.getDependencyList()) {
      Descriptors.FileDescriptor dependency = fileDescriptorsByName.get(depName);
      if (dependency != null) {
        dependencies.add(dependency);
        continue;
      }
      DescriptorProtos.FileDescriptorProto depProto =
          fds.getFileList().stream()
              .filter(f -> f.getName().equals(depName))
              .findFirst()
              .orElseThrow(() -> new IllegalArgumentException("dependency not found: " + depName));
      dependencies.add(buildFileDescriptor(depProto, fds, fileDescriptorsByName));
    }
    Descriptors.FileDescriptor fd =
        Descriptors.FileDescriptor.buildFrom(
            fdp, dependencies.toArray(new Descriptors.FileDescriptor[0]));
    fileDescriptorsByName.put(fdp.getName(), fd);
    return fd;
  }

  private static final class CacheKey {
    private final String commitID;
    private final String messageFQN;

    public CacheKey(String commitID, String messageFQN) {
      this.commitID = commitID;
      this.messageFQN = messageFQN;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      CacheKey cacheKey = (CacheKey) o;
      return Objects.equals(commitID, cacheKey.commitID)
          && Objects.equals(messageFQN, cacheKey.messageFQN);
    }

    @Override
    public int hashCode() {
      return Objects.hash(commitID, messageFQN);
    }

    @Override
    public String toString() {
      return String.format("CacheKey{commitID=%s, messageFQN=%s}", commitID, messageFQN);
    }
  }

  private static final class CacheEntry {
    final CompletableFuture<Descriptors.Descriptor> future = new CompletableFuture<>();
    final Instant createdAt;

    public CacheEntry(Instant createdAt) {
      this.createdAt = Objects.requireNonNull(createdAt);
    }

    public Descriptors.Descriptor join() {
      try {
        return future.join();
      } catch (CompletionException e) {
        // Unwrap the completion exception if possible.
        if (e.getCause() instanceof RuntimeException) {
          throw (RuntimeException) e.getCause();
        }
        throw e;
      }
    }
  }
}
