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
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Protobuf serializer for the given message.
 *
 * @param <T> Protobuf message type.
 */
public final class ProtoSerializer<T extends Message> implements Serializer<T> {

  /** Creates a new Protobuf serializer. */
  public ProtoSerializer() {}

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
    return serialize(topic, data);
  }
}
