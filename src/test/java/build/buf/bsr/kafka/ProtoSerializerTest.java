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

import build.buf.bsr.kafka.gen.opentelemetry.proto.logs.v1.LogRecord;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ProtoSerializerTest {

  @TempDir Path tempDir;

  @Test
  void serialize() throws InvalidProtocolBufferException {
    LogRecord event =
        LogRecord.newBuilder()
            .setTimeUnixNano(1_700_000_000_000_000_000L)
            .setSeverityText("INFO")
            .setEventName("demo")
            .build();
    try (ProtoSerializer<LogRecord> serializer = new ProtoSerializer<>()) {
      serializer.configure(Map.of(), false);
      RecordHeaders headers = new RecordHeaders();
      LogRecord roundTrip = LogRecord.parseFrom(serializer.serialize("my-topic", headers, event));
      Assertions.assertThat(roundTrip).isEqualTo(event);

      // Verify null data returns a null message and no headers are added.
      RecordHeaders nullHeaders = new RecordHeaders();
      Assertions.assertThat(serializer.serialize("my-topic", nullHeaders, null)).isNull();
      Assertions.assertThat(nullHeaders.toArray()).isEmpty();
    }
  }

  @Test
  void serializeWithHeaders_setsMessageFQNHeader() {
    LogRecord event =
        LogRecord.newBuilder()
            .setTimeUnixNano(1_700_000_000_000_000_000L)
            .setSeverityText("INFO")
            .setEventName("demo")
            .build();
    try (ProtoSerializer<LogRecord> serializer = new ProtoSerializer<>()) {
      serializer.configure(Map.of(), false);
      RecordHeaders headers = new RecordHeaders();
      serializer.serialize("my-topic", headers, event);

      // Message FQN header is always set.
      Header fqnHeader =
          headers.lastHeader(ProtoDeserializer.HEADER_BUF_REGISTRY_VALUE_SCHEMA_MESSAGE);
      Assertions.assertThat(fqnHeader).isNotNull();
      Assertions.assertThat(new String(fqnHeader.value(), StandardCharsets.UTF_8))
          .isEqualTo(LogRecord.getDescriptor().getFullName());

      // Commit header is not set when the class is not loaded from a JAR with Buf manifest
      // entries (e.g., test classes loaded from the build output directory).
      Assertions.assertThat(
              headers.lastHeader(ProtoDeserializer.HEADER_BUF_REGISTRY_VALUE_SCHEMA_COMMIT))
          .isNull();
    }
  }

  @Test
  void serializeWithHeaders_setsCommitHeaderFromManifest() throws IOException {
    String expectedCommit = "a1b2c3d4e5f6789012345678901234ab";
    URL jarUrl =
        BufManifestTest.createJarWithBufEntries(tempDir, "buf.build/acme/petapis", expectedCommit);
    BufManifest manifest = BufManifest.fromJarLocation(jarUrl);

    LogRecord event =
        LogRecord.newBuilder()
            .setTimeUnixNano(1_700_000_000_000_000_000L)
            .setSeverityText("INFO")
            .setEventName("demo")
            .build();
    try (ProtoSerializer<LogRecord> serializer = new ProtoSerializer<>(clazz -> manifest)) {
      serializer.configure(Map.of(), false);
      RecordHeaders headers = new RecordHeaders();
      serializer.serialize("my-topic", headers, event);

      Header commitHeader =
          headers.lastHeader(ProtoDeserializer.HEADER_BUF_REGISTRY_VALUE_SCHEMA_COMMIT);
      Assertions.assertThat(commitHeader).isNotNull();
      Assertions.assertThat(new String(commitHeader.value(), StandardCharsets.UTF_8))
          .isEqualTo(expectedCommit);

      Header fqnHeader =
          headers.lastHeader(ProtoDeserializer.HEADER_BUF_REGISTRY_VALUE_SCHEMA_MESSAGE);
      Assertions.assertThat(fqnHeader).isNotNull();
      Assertions.assertThat(new String(fqnHeader.value(), StandardCharsets.UTF_8))
          .isEqualTo(LogRecord.getDescriptor().getFullName());
    }
  }
}
