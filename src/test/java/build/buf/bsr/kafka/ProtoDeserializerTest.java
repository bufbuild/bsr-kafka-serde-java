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
import com.google.protobuf.DynamicMessage;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ProtoDeserializerTest {

  @Test
  void deserializeNoHeaders() {
    try (ProtoDeserializer<LogRecord> deserializer = new ProtoDeserializer<>()) {
      deserializer.configure(
          Map.of(
              ProtoDeserializerConfig.BSR_HOST_CONFIG,
              "localhost",
              ProtoDeserializerConfig.BSR_TOKEN_CONFIG,
              "mytoken",
              ProtoDeserializerConfig.VALUE_TYPE_CONFIG,
              LogRecord.class.getName()),
          false);
      LogRecord event =
          LogRecord.newBuilder()
              .setTimeUnixNano(1_700_000_000_000_000_000L)
              .setSeverityText("INFO")
              .setEventName("demo")
              .build();
      LogRecord deserialized = deserializer.deserialize("some-topic", event.toByteArray());
      Assertions.assertThat(deserialized).isEqualTo(event);
    }
  }

  @Test
  void deserializeWithHeadersNoValueType() {
    try (ProtoDeserializer<DynamicMessage> deserializer = new ProtoDeserializer<>()) {
      deserializer.configure(
          Map.of(
              ProtoDeserializerConfig.BSR_HOST_CONFIG,
              "localhost",
              ProtoDeserializerConfig.BSR_TOKEN_CONFIG,
              "mytoken"),
          false);
      BSRClient mockClient = Mockito.mock(BSRClient.class);
      deserializer.client = mockClient;
      String commitID = UUID.randomUUID().toString().replace("-", "");
      String messageFQN = LogRecord.class.getName();
      Mockito.when(mockClient.getMessageDescriptor(commitID, messageFQN))
          .thenReturn(LogRecord.getDescriptor());
      LogRecord event =
          LogRecord.newBuilder()
              .setTimeUnixNano(1_700_000_000_000_000_000L)
              .setSeverityText("INFO")
              .setEventName("demo")
              .build();
      RecordHeaders headers = new RecordHeaders();
      headers.add(
          ProtoDeserializer.HEADER_BUF_REGISTRY_VALUE_SCHEMA_COMMIT,
          commitID.getBytes(StandardCharsets.UTF_8));
      headers.add(
          ProtoDeserializer.HEADER_BUF_REGISTRY_VALUE_SCHEMA_MESSAGE,
          messageFQN.getBytes(StandardCharsets.UTF_8));
      DynamicMessage deserialized =
          deserializer.deserialize("some-topic", headers, event.toByteArray());
      Mockito.verify(mockClient).getMessageDescriptor(commitID, messageFQN);
      Assertions.assertThat(
              deserialized.getField(event.getDescriptorForType().findFieldByName("time_unix_nano")))
          .isEqualTo(event.getTimeUnixNano());
      Assertions.assertThat(
              deserialized.getField(event.getDescriptorForType().findFieldByName("severity_text")))
          .isEqualTo(event.getSeverityText());
    }
  }

  @Test
  void deserializeWithHeadersValueType() {
    try (ProtoDeserializer<LogRecord> deserializer = new ProtoDeserializer<>()) {
      deserializer.configure(
          Map.of(
              ProtoDeserializerConfig.BSR_HOST_CONFIG,
              "localhost",
              ProtoDeserializerConfig.BSR_TOKEN_CONFIG,
              "mytoken",
              ProtoDeserializerConfig.VALUE_TYPE_CONFIG,
              LogRecord.class.getName()),
          false);
      BSRClient mockClient = Mockito.mock(BSRClient.class);
      String commitID = UUID.randomUUID().toString().replace("-", "");
      String messageFQN = LogRecord.class.getName();
      Mockito.when(mockClient.getMessageDescriptor(commitID, messageFQN))
          .thenReturn(LogRecord.getDescriptor());
      deserializer.client = mockClient;
      LogRecord event =
          LogRecord.newBuilder()
              .setTimeUnixNano(1_700_000_000_000_000_000L)
              .setSeverityText("INFO")
              .setEventName("demo")
              .build();
      RecordHeaders headers = new RecordHeaders();
      headers.add(
          ProtoDeserializer.HEADER_BUF_REGISTRY_VALUE_SCHEMA_COMMIT,
          commitID.getBytes(StandardCharsets.UTF_8));
      headers.add(
          ProtoDeserializer.HEADER_BUF_REGISTRY_VALUE_SCHEMA_MESSAGE,
          messageFQN.getBytes(StandardCharsets.UTF_8));
      LogRecord deserialized = deserializer.deserialize("some-topic", headers, event.toByteArray());
      Mockito.verify(mockClient).getMessageDescriptor(commitID, messageFQN);
      Assertions.assertThat(deserialized).isEqualTo(event);
    }
  }
}
