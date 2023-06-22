package com.springkafka.kafka_app.democontroller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.springkafka.kafka_app.config.KafkaTopicDeletion;
import com.springkafka.kafka_app.event.Event;
import com.springkafka.kafka_app.service.kafka_consumer.Kafka_Consumer;
import com.springkafka.kafka_app.service.kafka_producer.Kafka_Producer;
import com.springkafka.kafka_app.utils.GroupEnum;
import com.springkafka.kafka_app.utils.LatencyCalculator;
import com.springkafka.kafka_app.utils.TopicEnum;
import com.springkafka.kafka_app.wrapper.ExecutorServiceWrapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


@RestController
@RequestMapping("/")
public class KafkaRestController {
    ExecutorServiceWrapper executorServiceWrapper;
    private final KafkaTopicDeletion kafkaTopicDeletion;
//    private final Kafka_Consumer kafka_consumer;
//    private final Kafka_Producer kafka_producer;

    @Autowired
    public KafkaRestController(ExecutorServiceWrapper executorServiceWrapper, KafkaTopicDeletion kafkaTopicDeletion, Kafka_Consumer kafka_consumer, Kafka_Producer kafka_producer) {
        this.executorServiceWrapper = executorServiceWrapper;
        this.executorServiceWrapper.setThreadCount(10);
        this.kafkaTopicDeletion = kafkaTopicDeletion;
//        this.kafka_consumer = kafka_consumer;
//        this.kafka_producer = kafka_producer;
    }

    @GetMapping("/producer")
    public void produceMessageForAddToCart(@RequestBody String data) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            Event event = objectMapper.readValue(data,Event.class);
            executorServiceWrapper.submit(Kafka_Producer.createN_Producer(event,1));
        } catch (Exception e){
            e.printStackTrace();
            System.out.println("data parsing failed with exception " + e);
        }
//        Producer<String,String> producer = Kafka_Producer.createProducer();
//        Kafka_Producer.sendMessage(message,producer,100);
//        executorService.submit(() -> Kafka_Producer.sendMessage(message, TopicEnum.TOPIC1.getTopicName(), producer,1));
    }

    @GetMapping("/consumer")
    public void consumeEvents() {
        executorServiceWrapper.submit(Kafka_Consumer.consumeEvents());
//        Consumer<String,String> consumer = Kafka_Consumer.createConsumer(GroupEnum.GROUP.getGroupName(), TopicEnum.TOPIC.getTopicName());
//        Kafka_Consumer.runConsumer(consumer);
//        executorService.submit(() -> Kafka_Producer.sendMessage(message, TopicEnum.TOPIC3.getTopicName(), producer, 1));
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

//    @GetMapping("/add-to-wishlist")
//    public void produceMessageForWishList(@RequestBody String message) {
//        executorService.submit( () ->Kafka_Producer.sendMessage(message, TopicEnum.TOPIC2.getTopicName(), producer,1));
//    }
//
//    @GetMapping("/read-messages")
//    public void readProducedMessages() {
//        executorService.submit(ConsumerRunner::runConsumer);
//    }

// wrapper for executor service-------
// singleton classes

// refactoring-------
// stop consumer------