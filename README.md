[![The Buf logo](.github/buf-logo.svg)][buf]

# bsr-kafka-serde-java

[![CI](https://github.com/bufbuild/bsr-kafka-serde-java/actions/workflows/ci.yaml/badge.svg)](https://github.com/bufbuild/bsr-kafka-serde-java/actions/workflows/ci.yaml)

[bsr-kafka-serde-java][bsr-kafka-serde-java] provides a Kafka serializer and deserializer in Java for working with schemas defined in the [Buf Schema Registry][bsr].
It pairs with [Bufstream's semantic validation][bufstream-semantic-validation] feature, using Kafka record headers to automatically convert record values to and from Protobuf.

## Usage

### Gradle

```kotlin
dependencies {
    implementation("build.buf.bsr.kafka:bsr-kafka-serde:<version>")
}
```

### Maven

```xml
<dependency>
    <groupId>build.buf.bsr.kafka</groupId>
    <artifactId>bsr-kafka-serde</artifactId>
    <version>${bsr-kafka-serde.version}</version>
</dependency>
```

## Producer

The producer requires no configuration except for setting the standard [ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG][kafka-producer-value-serializer].
Below is an example publishing a Protobuf `EmailUpdated` message to a topic using the BSR serializer.

<details>

<summary>Producer Example</summary>

```java
import build.buf.bsr.kafka.ProtoSerializer;
import build.buf.bsr.kafka.gen.bufstream.demo.v1.EmailUpdated;
import java.util.Properties;
import java.util.UUID;
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
        EmailUpdated emailUpdateMsg =
                EmailUpdated.newBuilder()
                        .setId(UUID.randomUUID().toString())
                        .setNewEmailAddress("newemail@mycompany.com")
                        .build();
        try (KafkaProducer<String, EmailUpdated> producer = new KafkaProducer<>(producerConfig)) {
            producer.send(new ProducerRecord<>("my-topic", emailUpdateMsg.getId(), emailUpdateMsg));
        }
    }
}
```

</details>

## Consumer

The consumer supports the following config settings:

| Setting      | Description                                                |
|--------------|------------------------------------------------------------|
| `bsr.host`   | Buf Schema Registry hostname (e.g. `buf.build`). Required. |
| `bsr.token`  | Buf Schema Registry API token.                             |
| `value.type` | The class name of the Protobuf message to decode into.     |

If the `value.type` is not specified, messages are decoded using DynamicMessage.

<details>

<summary>Consumer Example</summary>

```java
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
```

</details>

## Legal

Offered under the [Apache 2 license][license].

[bsr]: https://buf.build/docs/bsr/
[bsr-kafka-serde-java]: https://github.com/bufbuild/bsr-kafka-serde-java
[buf]: https://buf.build
[bufstream-semantic-validation]: https://buf.build/docs/bufstream/data-governance/semantic-validation/
[kafka-producer-value-serializer]: https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/producer/ProducerConfig.html#VALUE_SERIALIZER_CLASS_CONFIG
[license]: LICENSE
