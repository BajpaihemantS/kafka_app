package com.springkafka.kafka_app.democontroller;

import com.springkafka.kafka_app.config.KafkaTopicManager;
import com.springkafka.kafka_app.event.Event;
import com.springkafka.kafka_app.service.kafka_consumer.ConsumerKafka;
import com.springkafka.kafka_app.service.kafka_producer.ProducerKafka;
import com.springkafka.kafka_app.service.kafka_streams.KafkaStreamsService;
import com.springkafka.kafka_app.utils.*;
import com.springkafka.kafka_app.utils.Query.AttributeType;
import com.springkafka.kafka_app.utils.Query.Query;
import com.springkafka.kafka_app.utils.calculator.LatencyCalculator;
import com.springkafka.kafka_app.wrapper.CustomLogger;
import com.springkafka.kafka_app.wrapper.ExecutorServiceWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * The controller class which has specific endpoints to create consumers and producers and get the stats of latency.
 * It is also running a separate thread for graceful shutdown
 *
 */

@RestController
@RequestMapping("/")
public class KafkaRestController extends CustomLogger {
    private final ExecutorServiceWrapper executorServiceWrapper;
    private final ConsumerKafka kafka_consumer;
    private final ProducerKafka kafka_producer;
    private final EventGenerator eventGenerator;
    private final KafkaStreamsService kafkaStreamsService;
    private final KafkaTopicManager kafkaTopicManager;
    private List<String> topicList;

    @Autowired
    public KafkaRestController(ExecutorServiceWrapper executorServiceWrapper, ConsumerKafka kafka_consumer, ProducerKafka kafka_producer, KafkaStreamsService kafkaStreamsService, KafkaTopicManager kafkaTopicManager) {
        this.executorServiceWrapper = executorServiceWrapper;
        this.executorServiceWrapper.setThreadCount(100);
        this.kafka_consumer = kafka_consumer;
        this.kafka_producer = kafka_producer;
        eventGenerator = new EventGenerator();
        this.kafkaStreamsService = kafkaStreamsService;
        this.kafkaTopicManager = kafkaTopicManager;
        topicList = new ArrayList<>();
        Runtime.getRuntime().addShutdownHook( new Thread(this::shutdown));
    }

    @GetMapping("/producer")
    public void produceEvents() {
        List<Event> eventList = eventGenerator.generateNEvents(ServiceProperties.MAX_EVENTS);
        executorServiceWrapper.submit(kafka_producer.createN_Producer(eventList));
    }

    @GetMapping("/consumer")
    public void consumeEvents() {
        String topic = TopicEnum.TOPIC.getTopicName();
        executorServiceWrapper.submit(kafka_consumer.consumeEvents(topic));
    }

    @GetMapping("/stats")
    public String getLatencyStats(){
        return LatencyCalculator.printStats();
    }


    @GetMapping("/getEventsInTopic")
    public void getAllRequiredEvents(@RequestBody Query query){
        StringBuilder eventTopic = new StringBuilder(TopicEnum.TOPIC.getTopicName());
        for(AttributeType attributeType : query.getAttributeTypeList()) eventTopic.append(attributeType.getType());
        String topic = String.valueOf(eventTopic);
        String outputTopic  = eventTopic + "_filter";
//        topicList.add(topic);
//        topicList.add(outputTopic);
//        kafkaTopicManager.createTopics(topicList);
        info("topic to delete is {} and {}", topic,outputTopic);
        executorServiceWrapper.submit(kafkaStreamsService.startStreams(query, TopicEnum.TOPIC.getTopicName(), outputTopic));
        executorServiceWrapper.submit(kafka_consumer.consumeEvents(outputTopic));
    }

    private void shutdown() {
        info("Initiating shutdown protocol. Killing all processes.......");
        topicList.add(TopicEnum.TOPIC.getTopicName());
        info("the topic to be deleted is {}",topicList.get(0));
        kafkaTopicManager.deleteTopics(topicList);
    }
}

