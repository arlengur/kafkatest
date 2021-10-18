package ru.alen.kafkatest;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import ru.alen.kafkatest.consumer.MyConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SpringBootTest()
class KafkaContainerIntegrationTest {
    private static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

    @Value("${kafka.topicName}")
    private String topic;

    @Autowired
    KafkaTemplate<String, String> producer;

    @SpyBean
    private MyConsumer consumer;

    @Captor
    ArgumentCaptor<String> userArgumentCaptor;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        kafka.start();
        registry.add("kafka.bootstrapServers", () -> kafka.getHost() + ":" + kafka.getFirstMappedPort());
    }

    @Test
    public void testLogKafkaMessages() throws Exception {
        String message = "some string";
        producer.send(new ProducerRecord<>(topic, message)).get();

        // Read the message and assert its properties
        verify(consumer, timeout(5000).times(1)).listen(userArgumentCaptor.capture());

        assertEquals(message, userArgumentCaptor.getValue());
    }
}