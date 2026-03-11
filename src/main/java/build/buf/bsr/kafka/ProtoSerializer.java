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

import com.google.protobuf.Message;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Protobuf serializer for the given message type.
 *
 * <p>When called with the {@link #serialize(String, org.apache.kafka.common.header.Headers, Object)
 * Headers overload}, this serializer always sets the {@value
 * ProtoDeserializer#HEADER_BUF_REGISTRY_VALUE_SCHEMA_MESSAGE} header from the message descriptor.
 * If the class was loaded from a JAR containing a {@value BufManifest#ATTRIBUTE_BUF_MODULE_COMMIT}
 * manifest entry, it also sets the {@value
 * ProtoDeserializer#HEADER_BUF_REGISTRY_VALUE_SCHEMA_COMMIT} header. See {@link BufManifest} for
 * how to add that entry to the JAR.
 *
 * @param <T> Protobuf message type.
 */
public final class ProtoSerializer<T extends Message> implements Serializer<T> {

  private final Function<Class<?>, BufManifest> manifestResolver;

  /** Creates a new Protobuf serializer. */
  public ProtoSerializer() {
    this(BufManifest::forClass);
  }

  ProtoSerializer(Function<Class<?>, BufManifest> manifestResolver) {
    this.manifestResolver = manifestResolver;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    if (isKey) {
      throw new SerializationException("only Kafka values are supported for serialization");
    }
  }

  @Override
  public byte[] serialize(String topic, T data) {
    if (data == null) {
      return null;
    }
    return data.toByteArray();
  }

  @Override
  public byte[] serialize(String topic, Headers headers, T data) {
    if (data == null) {
      return null;
    }
    String messageFQN = data.getDescriptorForType().getFullName();
    headers.add(
        ProtoDeserializer.HEADER_BUF_REGISTRY_VALUE_SCHEMA_MESSAGE,
        messageFQN.getBytes(StandardCharsets.UTF_8));
    String moduleCommit = manifestResolver.apply(data.getClass()).getModuleCommit();
    if (moduleCommit != null) {
      headers.add(
          ProtoDeserializer.HEADER_BUF_REGISTRY_VALUE_SCHEMA_COMMIT,
          moduleCommit.getBytes(StandardCharsets.UTF_8));
    }
    return data.toByteArray();
  }
}
