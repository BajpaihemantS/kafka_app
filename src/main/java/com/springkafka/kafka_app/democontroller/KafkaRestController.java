package com.springkafka.kafka_app.democontroller;

import com.springkafka.kafka_app.service.kafka_consumer.ConsumerKafka;
import com.springkafka.kafka_app.service.kafka_producer.ProducerKafka;
import com.springkafka.kafka_app.utils.EventGenerator;
import com.springkafka.kafka_app.utils.ServiceProperties;
import com.springkafka.kafka_app.utils.TopicEnum;
import com.springkafka.kafka_app.wrapper.CustomLogger;
import com.springkafka.kafka_app.config.KafkaTopicManager;
import com.springkafka.kafka_app.event.Event;
import com.springkafka.kafka_app.service.kafka_streams.KafkaStreamsService;
import com.springkafka.kafka_app.service.scheduler.Scheduler;
import com.springkafka.kafka_app.utils.Query.Query;
import com.springkafka.kafka_app.utils.calculator.LatencyCalculator;
import com.springkafka.kafka_app.wrapper.ExecutorServiceWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final KafkaTopicManager kafkaTopicManager;
    private final ConsumerKafka kafka_consumer;
    private final ProducerKafka kafka_producer;
    private final EventGenerator eventGenerator;
    private final KafkaStreamsService kafkaStreamsService;
    private final List<String> topicList;
    private final AtomicInteger queryCount;


    @Autowired
    public KafkaRestController(ExecutorServiceWrapper executorServiceWrapper, KafkaTopicManager kafkaTopicManager, ConsumerKafka kafka_consumer, ProducerKafka kafka_producer, KafkaStreamsService kafkaStreamsService) {
        this.executorServiceWrapper = executorServiceWrapper;
        this.executorServiceWrapper.setThreadCount(100);
        this.kafkaTopicManager = kafkaTopicManager;
        this.kafka_consumer = kafka_consumer;
        this.kafka_producer = kafka_producer;
        eventGenerator = new EventGenerator();
        this.kafkaStreamsService = kafkaStreamsService;
        topicList = new ArrayList<>();
        this.queryCount = new AtomicInteger();

        Runtime.getRuntime().addShutdownHook( new Thread(this::shutdown));
    }

    @GetMapping("/producer")
    public void produceEvents() {
        List<Event> eventList = eventGenerator.generateNEvents(ServiceProperties.MAX_EVENTS);
        topicList.add(TopicEnum.TOPIC.getTopicName());
        executorServiceWrapper.submit(kafka_producer.createN_Producer(eventList));
    }

    @GetMapping("/consumer")
    public void consumeEvents() {
        String topic = TopicEnum.TOPIC.getTopicName();
        executorServiceWrapper.submit(kafka_consumer.createN_Consumer(1,topic, new Query()));
    }

    @GetMapping("/stats")
    public String getLatencyStats(){
        return LatencyCalculator.printStats();
    }

    /**
     * This class is the end point which calls for the new streams and a new consumer to output the result using a scheduler
     * The topic is created dynamically as it needs to be distinct for multiple queries to run at the same time.
     * @param query tells us the specified query
     */
    @GetMapping("/getUsersFromQuery")
    public void getAllRequiredEvents(@RequestBody Query query){
        queryCount.incrementAndGet(); // this tells us the exact query number
        String outputTopic  = TopicEnum.TOPIC.getTopicName() + queryCount; // Here a new output topic name is generated depending upon the query number
        topicList.add(outputTopic);
        executorServiceWrapper.submit(kafkaStreamsService.startStreams(query,outputTopic));
        Scheduler scheduler = new Scheduler(kafka_consumer,query,outputTopic);
        executorServiceWrapper.submit(() ->scheduler.startScheduling(queryCount));
    }

    private void shutdown() {
        info("Initiating shutdown protocol. Killing all processes.......");
        kafkaTopicManager.deleteTopics(topicList); // This will delete all the topics which have been created
    }
}

