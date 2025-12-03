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

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * ProtoDeserializer is a Kafka deserializer for Protobuf encoded messages. Instead of using a
 * custom wire format, the deserializer uses {@value #HEADER_BUF_REGISTRY_VALUE_SCHEMA_COMMIT} and
 * {@value #HEADER_BUF_REGISTRY_VALUE_SCHEMA_MESSAGE} headers to look up the schema from the Buf
 * Schema Registry.
 *
 * @param <T> Type of message.
 */
public class ProtoDeserializer<T extends Message> implements Deserializer<T> {

  public static final String HEADER_BUF_REGISTRY_VALUE_SCHEMA_COMMIT =
      "buf.registry.value.schema.commit";
  public static final String HEADER_BUF_REGISTRY_VALUE_SCHEMA_MESSAGE =
      "buf.registry.value.schema.message";

  BSRClient client;
  Method parseFromMethod;
  String expectedMessageFQN;

  /** Creates a new deserializer. */
  public ProtoDeserializer() {}

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    if (isKey) {
      throw new SerializationException("only Kafka values are supported for deserialization");
    }
    configure(new ProtoDeserializerConfig(configs));
  }

  private void configure(ProtoDeserializerConfig config) {
    String host = config.getString(ProtoDeserializerConfig.BSR_HOST_CONFIG);
    if (host == null || host.isEmpty()) {
      throw new SerializationException(
          String.format(
              "required %s config not specified", ProtoDeserializerConfig.BSR_HOST_CONFIG));
    }
    String token = config.getString(ProtoDeserializerConfig.BSR_TOKEN_CONFIG);
    this.client = new Client(host, token);
    Class<T> valueType = (Class<T>) config.getClass(ProtoDeserializerConfig.VALUE_TYPE_CONFIG);
    if (valueType != null && !Object.class.equals(valueType)) {
      try {
        this.parseFromMethod = valueType.getDeclaredMethod("parseFrom", byte[].class);
        Descriptors.Descriptor descriptor =
            (Descriptors.Descriptor) valueType.getDeclaredMethod("getDescriptor").invoke(null);
        this.expectedMessageFQN = descriptor.getFullName();
      } catch (Exception e) {
        throw new SerializationException(
            String.format(
                "%s is not a valid Protobuf message class",
                ProtoDeserializerConfig.VALUE_TYPE_CONFIG));
      }
    }
  }

  @Override
  public T deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }
    if (this.parseFromMethod == null) {
      throw new SerializationException(
          "unable to deserialize data without a valid value.type or headers");
    }
    try {
      Object message = this.parseFromMethod.invoke(null, data);
      if (message == null) {
        return null;
      }
      if (!MessageLite.class.isAssignableFrom(message.getClass())) {
        throw new SerializationException("deserialized message is not a Protobuf message");
      }
      return (T) message;
    } catch (ClassCastException | IllegalAccessException | InvocationTargetException e) {
      throw new SerializationException("failed to deserialize message", e);
    }
  }

  @Override
  public T deserialize(String topic, Headers headers, byte[] data) {
    Header headerCommitID = headers.lastHeader(HEADER_BUF_REGISTRY_VALUE_SCHEMA_COMMIT);
    Header headerMessageFQN = headers.lastHeader(HEADER_BUF_REGISTRY_VALUE_SCHEMA_MESSAGE);
    if (headerCommitID == null || headerMessageFQN == null) {
      return deserialize(topic, data);
    }
    try {
      String commitID = new String(headerCommitID.value(), StandardCharsets.UTF_8);
      String messageFQN = new String(headerMessageFQN.value(), StandardCharsets.UTF_8);
      Descriptors.Descriptor messageDescriptor = client.getMessageDescriptor(commitID, messageFQN);
      if (parseFromMethod == null) {
        return (T) DynamicMessage.parseFrom(messageDescriptor, data);
      }
      T message = (T) parseFromMethod.invoke(null, data);
      if (!message.getDescriptorForType().getFullName().equals(expectedMessageFQN)) {
        throw new SerializationException(
            "expected message " + expectedMessageFQN + " but got " + messageFQN);
      }
      return message;
    } catch (Exception e) {
      throw new SerializationException("failed to deserialize message", e);
    }
  }
}
