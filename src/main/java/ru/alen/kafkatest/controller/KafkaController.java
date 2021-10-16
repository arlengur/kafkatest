package ru.alen.kafkatest.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.alen.kafkatest.configuration.KafkaProperties;
import ru.alen.kafkatest.consumer.MyConsumer;
import ru.alen.kafkatest.dto.MyDTO;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

@RestController
public class KafkaController {
    private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);

    private final KafkaProperties properties;

    private KafkaTemplate<String, String> template;
    private MyConsumer consumer;
    final ObjectMapper objectMapper;

    public KafkaController(KafkaTemplate<String, String> template, MyConsumer consumer,
            ObjectMapper objectMapper, KafkaProperties properties) {
        this.template = template;
        this.consumer = consumer;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    @SneakyThrows
    @GetMapping("/kafka/produce")
    public void produce(@RequestParam String msg) {
        MyDTO myDTO = new MyDTO("id", "state", Timestamp.valueOf(LocalDateTime.now()).getTime(), msg);
        logger.info(String.format("#### -> Producing message -> %s", objectMapper.writeValueAsString(myDTO)));
        template.send(properties.getTopicName(), objectMapper.writeValueAsString(myDTO));
    }

    @GetMapping("/kafka/messages")
    public List<String> getMessages() {
        return consumer.getMessages();
    }
}
