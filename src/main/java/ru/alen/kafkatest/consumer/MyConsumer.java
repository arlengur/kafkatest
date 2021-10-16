package ru.alen.kafkatest.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class MyConsumer {
    private final Logger logger = LoggerFactory.getLogger(MyConsumer.class);
    private final List<String> messages = new ArrayList<>();

    @KafkaListener(topics = "${kafka.topicName}")
    public void listen(@Payload String message) {
        synchronized (messages) {
            logger.info(String.format("#### -> Consumed message -> %s", message));
            messages.add(message);
        }
    }
    public List<String> getMessages() {
        return messages;
    }
}
