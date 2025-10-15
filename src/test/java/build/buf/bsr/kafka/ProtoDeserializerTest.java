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

import build.buf.bsr.kafka.gen.bufstream.demo.v1.EmailUpdated;
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
    try (ProtoDeserializer<EmailUpdated> deserializer = new ProtoDeserializer<>()) {
      deserializer.configure(
          Map.of(
              ProtoDeserializerConfig.BSR_HOST_CONFIG,
              "localhost",
              ProtoDeserializerConfig.BSR_TOKEN_CONFIG,
              "mytoken",
              ProtoDeserializerConfig.VALUE_TYPE_CONFIG,
              EmailUpdated.class.getName()),
          false);
      EmailUpdated event =
          EmailUpdated.newBuilder()
              .setId(UUID.randomUUID().toString())
              .setNewEmailAddress("myemail@host.com")
              .build();
      EmailUpdated deserialized = deserializer.deserialize("some-topic", event.toByteArray());
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
      String messageFQN = EmailUpdated.class.getName();
      Mockito.when(mockClient.getMessageDescriptor(commitID, messageFQN))
          .thenReturn(EmailUpdated.getDescriptor());
      EmailUpdated event =
          EmailUpdated.newBuilder()
              .setId(UUID.randomUUID().toString())
              .setNewEmailAddress("myemail@host.com")
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
              deserialized.getField(event.getDescriptorForType().findFieldByName("id")))
          .isEqualTo(event.getId());
      Assertions.assertThat(
              deserialized.getField(
                  event.getDescriptorForType().findFieldByName("new_email_address")))
          .isEqualTo(event.getNewEmailAddress());
    }
  }

  @Test
  void deserializeWithHeadersValueType() {
    try (ProtoDeserializer<EmailUpdated> deserializer = new ProtoDeserializer<>()) {
      deserializer.configure(
          Map.of(
              ProtoDeserializerConfig.BSR_HOST_CONFIG,
              "localhost",
              ProtoDeserializerConfig.BSR_TOKEN_CONFIG,
              "mytoken",
              ProtoDeserializerConfig.VALUE_TYPE_CONFIG,
              EmailUpdated.class.getName()),
          false);
      BSRClient mockClient = Mockito.mock(BSRClient.class);
      String commitID = UUID.randomUUID().toString().replace("-", "");
      String messageFQN = EmailUpdated.class.getName();
      Mockito.when(mockClient.getMessageDescriptor(commitID, messageFQN))
          .thenReturn(EmailUpdated.getDescriptor());
      deserializer.client = mockClient;
      EmailUpdated event =
          EmailUpdated.newBuilder()
              .setId(UUID.randomUUID().toString())
              .setNewEmailAddress("myemail@host.com")
              .build();
      RecordHeaders headers = new RecordHeaders();
      headers.add(
          ProtoDeserializer.HEADER_BUF_REGISTRY_VALUE_SCHEMA_COMMIT,
          commitID.getBytes(StandardCharsets.UTF_8));
      headers.add(
          ProtoDeserializer.HEADER_BUF_REGISTRY_VALUE_SCHEMA_MESSAGE,
          messageFQN.getBytes(StandardCharsets.UTF_8));
      EmailUpdated deserialized =
          deserializer.deserialize("some-topic", headers, event.toByteArray());
      Mockito.verify(mockClient).getMessageDescriptor(commitID, messageFQN);
      Assertions.assertThat(deserialized).isEqualTo(event);
    }
  }
}
