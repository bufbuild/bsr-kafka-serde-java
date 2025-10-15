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

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@Testcontainers
public class SerdeTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(SerdeTest.class);

  @Container
  private final GenericContainer<?> bufstream =
      new FixedHostPortGenericContainer<>("bufbuild/bufstream:0.4.4")
          .withCopyFileToContainer(
              MountableFile.forClasspathResource("bufstream.yaml"), "/bufstream.yaml")
          .withCommand("serve", "--config", "/bufstream.yaml")
          .withLogConsumer(new Slf4jLogConsumer(LOGGER))
          .withFixedExposedPort(29092, 9092)
          .waitingFor(
              new AbstractWaitStrategy() {
                @Override
                protected void waitUntilReady() {
                  List<String> command =
                      List.of(
                          "/usr/local/bin/bufstream",
                          "admin",
                          "status",
                          "--exit-code",
                          "--url",
                          "http://127.0.0.1:9089");
                  final Instant started = Instant.now();
                  while (!Instant.now().isAfter(started.plus(startupTimeout))) {
                    try {
                      int exitCode =
                          waitStrategyTarget
                              .execInContainer(command.toArray(new String[0]))
                              .getExitCode();
                      if (exitCode == 0) {
                        return;
                      }
                    } catch (Exception e) {
                      LOGGER.warn("failed to run bufstream admin status", e);
                    }
                  }
                  throw new ContainerLaunchException(
                      "Timed out waiting for container to execute `" + command + "` successfully.");
                }
              });

  @Test
  @EnabledIfSystemProperty(named = "bsr.integration", matches = "true")
  @Timeout(value = 1, unit = TimeUnit.MINUTES)
  void testSerde() throws Exception {
    String topicName = "bsr-test";
    int kafkaPort = bufstream.getMappedPort(9092);
    Properties baseConfig = new Properties();
    baseConfig.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + kafkaPort);
    baseConfig.setProperty(AdminClientConfig.CLIENT_ID_CONFIG, "myclient,host_override=localhost");
    try (AdminClient adminClient = AdminClient.create(baseConfig)) {
      adminClient
          .createTopics(
              List.of(
                  new NewTopic(topicName, 1, (short) 1)
                      .configs(
                          Map.of(
                              "bufstream.validate.mode",
                              "reject",
                              "buf.registry.value.schema.message",
                              "bufstream.demo.v1.EmailUpdated",
                              "buf.registry.value.schema.module",
                              "demo.buf.dev/bufbuild/bufstream-demo"))))
          .all()
          .get();
    }

    Properties producerConfig = new Properties();
    producerConfig.putAll(baseConfig);
    producerConfig.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerConfig.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtoSerializer.class.getName());

    Path resourcePath = Paths.get("src/test/resources/email-updated-fds.binpb");
    DescriptorProtos.FileDescriptorSet savedFds;
    try (FileInputStream fis = new FileInputStream(resourcePath.toFile())) {
      savedFds = DescriptorProtos.FileDescriptorSet.parseFrom(fis);
    }
    Descriptors.Descriptor descriptor =
        Client.findMessageDescriptor(savedFds, "bufstream.demo.v1.EmailUpdated");
    Assertions.assertThat(descriptor).isNotNull();
    DynamicMessage message =
        DynamicMessage.newBuilder(descriptor)
            .setField(descriptor.findFieldByName("id"), UUID.randomUUID().toString())
            .setField(descriptor.findFieldByName("new_email_address"), "testemail@test.com")
            .build();
    try (Producer<String, Message> producer = new KafkaProducer<>(producerConfig)) {
      producer.send(new ProducerRecord<>(topicName, message));
      producer.flush();
    }

    Properties consumerConfig = new Properties();
    consumerConfig.putAll(baseConfig);
    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testgroup");
    consumerConfig.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerConfig.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ProtoDeserializer.class.getName());
    consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.setProperty(ProtoDeserializerConfig.BSR_HOST_CONFIG, "demo.buf.dev");

    try (Consumer<String, Message> consumer = new KafkaConsumer<>(consumerConfig)) {
      consumer.subscribe(List.of(topicName));
      Message consumedMessage = null;
      while (consumedMessage == null) {
        ConsumerRecords<String, Message> records = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, Message> record : records) {
          Assertions.assertThat(consumedMessage).isNull();
          consumedMessage = record.value();
        }
      }
      Descriptors.Descriptor consumedDescriptor = consumedMessage.getDescriptorForType();
      Assertions.assertThat(consumedDescriptor.getFullName())
          .isEqualTo(message.getDescriptorForType().getFullName());
      Assertions.assertThat(consumedMessage.getField(consumedDescriptor.findFieldByName("id")))
          .isEqualTo(message.getField(descriptor.findFieldByName("id")));
      Assertions.assertThat(
              consumedMessage.getField(consumedDescriptor.findFieldByName("new_email_address")))
          .isEqualTo(message.getField(descriptor.findFieldByName("new_email_address")));
    }
  }
}
