package ru.alen.kafkatest;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest()
class KafkaContainerTest {
    private static final DockerImageName KAFKA_TEST_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:6.2.1");

    @Value("${kafka.topicName}")
    private String topic;

    @Test
    public void givenEmbeddedKafkaBroker_whenSendingtoDefaultTemplate_thenMessageReceived() throws Exception {
        KafkaContainer kafka = new KafkaContainer(KAFKA_TEST_IMAGE);
        kafka.start();
        try (AdminClient adminClient = getAdminClient(kafka);
                KafkaProducer<String, String> producer = getProducer(kafka);
                KafkaConsumer<String, String> consumer = getConsumer(kafka)) {

            Collection<NewTopic> topics = singletonList(new NewTopic(topic, 1, (short) 1));
            adminClient.createTopics(topics).all().get(20, TimeUnit.SECONDS);

            consumer.subscribe(Collections.singleton(topic));

            producer.send(new ProducerRecord<>(topic, "Sending with default template")).get();

            Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () -> {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    return false;
                }

                assertThat(records).hasSize(1)
                        .extracting(ConsumerRecord::value)
                        .containsExactly("Sending with default template");

                return true;
            });

            consumer.unsubscribe();
        }
    }

    private AdminClient getAdminClient(KafkaContainer kafka) {
        return AdminClient.create(
                ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));
    }

    private KafkaProducer<String, String> getProducer(KafkaContainer kafka) {
        return new KafkaProducer<>(ImmutableMap.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()), new StringSerializer(),
                new StringSerializer());
    }

    private KafkaConsumer<String, String> getConsumer(KafkaContainer kafka) {
        return new KafkaConsumer<>(ImmutableMap.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"), new StringDeserializer(), new StringDeserializer());
    }

}