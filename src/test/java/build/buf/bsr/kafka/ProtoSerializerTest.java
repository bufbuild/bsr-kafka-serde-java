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
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class ProtoSerializerTest {
  @Test
  void serialize() throws InvalidProtocolBufferException {
    EmailUpdated event =
        EmailUpdated.newBuilder()
            .setId(UUID.randomUUID().toString())
            .setNewEmailAddress("myemail@host.com")
            .build();
    try (ProtoSerializer<EmailUpdated> serializer = new ProtoSerializer<>()) {
      serializer.configure(Map.of(), false);
      EmailUpdated roundTrip =
          EmailUpdated.parseFrom(serializer.serialize("my-topic", new RecordHeaders(), event));
      Assertions.assertThat(roundTrip).isEqualTo(event);

      // Verify null data returns a null message
      Assertions.assertThat(serializer.serialize("my-topic", new RecordHeaders(), null)).isNull();
    }
  }
}
