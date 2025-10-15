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

package build.buf.bsr.kafka.examples;

import build.buf.bsr.kafka.ProtoDeserializer;
import build.buf.bsr.kafka.ProtoDeserializerConfig;
import com.google.protobuf.Message;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ExampleConsumer {
  public static void main(String[] args) {
    Properties consumerConfig = new Properties();
    consumerConfig.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testgroup");
    consumerConfig.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerConfig.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ProtoDeserializer.class.getName());
    // Replace the following two configs with the BSR instance and bot user API token for the BSR
    consumerConfig.setProperty(ProtoDeserializerConfig.BSR_HOST_CONFIG, "<bsr-host>");
    consumerConfig.setProperty(ProtoDeserializerConfig.BSR_TOKEN_CONFIG, "<api-token>");

    try (Consumer<String, Message> consumer = new KafkaConsumer<>(consumerConfig)) {
      consumer.subscribe(List.of("my-topic"));
      ConsumerRecords<String, Message> records = consumer.poll(Duration.ofSeconds(1));
      for (ConsumerRecord<String, Message> record : records) {
        System.out.println(record.value());
      }
    }
  }
}
