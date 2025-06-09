package com.messagebrokers.message_broker_lab.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.TestPropertySource;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=localhost:9092",
        "spring.kafka.consumer.auto-offset-reset=earliest"
})
class KafkaConnectionTest {

    @Autowired
    private KafkaTemplate<String, Map<String, String>> kafkaTemplate;

    private final BlockingQueue<Map<String, String>> messages = new LinkedBlockingQueue<>();

    @KafkaListener(topics = "test-topic", groupId = "test-group")
    public void listen(Map<String, String> message) {
        messages.add(message);
    }

    @Test
    void testKafkaConnectionAndJsonMapMessage() throws InterruptedException {
        Map<String, String> sentMessage = Map.of("key", "value");
        kafkaTemplate.send("test-topic", sentMessage);

        Map<String, String> received = messages.poll(10, TimeUnit.SECONDS);
        assertNotNull(received, "Did not receive any message from Kafka");
        assertEquals("value", received.get("key"));
    }
}
