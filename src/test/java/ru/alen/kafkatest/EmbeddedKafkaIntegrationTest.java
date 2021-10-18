package ru.alen.kafkatest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import ru.alen.kafkatest.consumer.MyConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@EmbeddedKafka
@SpringBootTest(properties = "kafka.bootstrapServers=${spring.embedded.kafka.brokers}")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EmbeddedKafkaIntegrationTest {
    @Value("${kafka.topicName}")
    private String topicName;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @SpyBean
    private MyConsumer userKafkaConsumer;

    @Captor
    ArgumentCaptor<String> userArgumentCaptor;

    @Test
    void testLogKafkaMessages() {
        String message = "some string";
        kafkaTemplate.send(topicName, message);

        // Read the message and assert its properties
        verify(userKafkaConsumer, timeout(5000).times(1)).listen(userArgumentCaptor.capture());

        assertEquals(message, userArgumentCaptor.getValue());
    }
}