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

import build.buf.bsr.kafka.ProtoSerializer;
import build.buf.bsr.kafka.gen.opentelemetry.proto.logs.v1.LogRecord;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ExampleProducer {
  public static void main(String[] args) {
    Properties producerConfig = new Properties();
    producerConfig.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    producerConfig.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    // Set the value serializer to encode the message as Protobuf bytes
    producerConfig.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtoSerializer.class.getName());
    LogRecord logRecord =
        LogRecord.newBuilder()
            .setTimeUnixNano(TimeUnit.SECONDS.toNanos(Instant.now().getEpochSecond()))
            .setSeverityText("INFO")
            .setEventName("demo")
            .build();
    try (KafkaProducer<String, LogRecord> producer = new KafkaProducer<>(producerConfig)) {
      producer.send(new ProducerRecord<>("my-topic", "demo", logRecord));
    }
  }
}
