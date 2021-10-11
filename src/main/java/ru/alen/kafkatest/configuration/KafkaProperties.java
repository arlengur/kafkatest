package ru.alen.kafkatest.configuration;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
public class KafkaProperties {
    @Value("${kafka.bootstrapServers}")
    private String bootstrapServers;
    @Value("${kafka.groupID}")
    private String groupID;
}
