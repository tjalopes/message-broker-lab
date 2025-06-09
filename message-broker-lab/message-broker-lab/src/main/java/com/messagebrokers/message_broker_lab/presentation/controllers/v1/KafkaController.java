package com.messagebrokers.message_broker_lab.presentation.controllers.v1;

import com.messagebrokers.message_broker_lab.application.dtos.requests.TopicOneRequest;
import com.messagebrokers.message_broker_lab.application.dtos.requests.TopicTwoRequest;
import com.messagebrokers.message_broker_lab.application.services.kafka.KafkaProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    private final KafkaProducerService kafkaProducerService;

    public KafkaController(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @PostMapping("/producer/topic-one")
    public ResponseEntity<Void> testTopicOne(@RequestBody TopicOneRequest topicOneRequest) {
        kafkaProducerService.testTopicOne(topicOneRequest);

        return ResponseEntity.ok().build();
    }

    @PostMapping("/producer/topic-two")
    public ResponseEntity<Void> testTopicTwo(@RequestBody TopicTwoRequest topicTwoRequest) {
        kafkaProducerService.testTopicTwo(topicTwoRequest);

        return ResponseEntity.ok().build();
    }
}
