package com.messagebrokers.message_broker_lab.presentation.controllers.v1;

import com.messagebrokers.message_broker_lab.application.dtos.requests.TopicOneRequest;
import com.messagebrokers.message_broker_lab.application.dtos.requests.TopicTwoRequest;
import com.messagebrokers.message_broker_lab.application.services.KafkaService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    private final KafkaService kafkaService;

    public KafkaController(KafkaService kafkaService) {
        this.kafkaService = kafkaService;
    }

    @PostMapping("/topic-one")
    public void testTopicOne(@RequestBody TopicOneRequest topicOneRequest) {
        kafkaService.testTopicOne(topicOneRequest);
    }

    @PostMapping("/topic-two")
    public void testTopicTwo(@RequestBody TopicTwoRequest topicTwoRequest) {
        kafkaService.testTopicTwo(topicTwoRequest);
    }
}
