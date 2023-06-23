package com.springkafka.kafka_app.democontroller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.springkafka.kafka_app.config.KafkaTopicDeletion;
import com.springkafka.kafka_app.event.Event;
import com.springkafka.kafka_app.service.kafka_consumer.Kafka_Consumer;
import com.springkafka.kafka_app.service.kafka_producer.Kafka_Producer;
import com.springkafka.kafka_app.utils.LatencyCalculator;
import com.springkafka.kafka_app.wrapper.ExecutorServiceWrapper;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;


@RestController
@RequestMapping("/")
public class KafkaRestController {
    ExecutorServiceWrapper executorServiceWrapper;
    private final KafkaTopicDeletion kafkaTopicDeletion;
    private final Kafka_Consumer kafka_consumer;
    private final Kafka_Producer kafka_producer;

    @Autowired
    public KafkaRestController(ExecutorServiceWrapper executorServiceWrapper, KafkaTopicDeletion kafkaTopicDeletion, Kafka_Consumer kafka_consumer, Kafka_Producer kafka_producer) {
        this.executorServiceWrapper = executorServiceWrapper;
        this.executorServiceWrapper.setThreadCount(2);
        this.kafkaTopicDeletion = kafkaTopicDeletion;
        this.kafka_consumer = kafka_consumer;
        this.kafka_producer = kafka_producer;
    }

    @GetMapping("/producer")
    public void produceMessageForAddToCart(@RequestBody ArrayList<Object> data) throws JsonProcessingException {
//        System.out.println(data.get(0).toString());

            executorServiceWrapper.submit(kafka_producer.createN_Producer(data));

    }

    @GetMapping("/consumer")
    public void consumeEvents() {

        executorServiceWrapper.submit(kafka_consumer.consumeEvents());
    }

    @GetMapping("/stats")
    public String getLatencyStats(){

        return LatencyCalculator.printStats();

    }


    @PostConstruct
    public void clearingTopics(){
        kafkaTopicDeletion.stop();

    }
}
