package org.kafkaApp.democontroller;

import org.kafkaApp.config.KafkaTopicManager;
import org.kafkaApp.event.Event;
import org.kafkaApp.service.kafka_producer.ProducerKafka;
import org.kafkaApp.utils.EventGenerator;
import org.kafkaApp.utils.ServiceProperties;
import org.kafkaApp.utils.TopicEnum;
import org.kafkaApp.wrapper.CustomLogger;
import org.kafkaApp.wrapper.ExecutorServiceWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
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
public class ProducerController extends CustomLogger {
    private final ExecutorServiceWrapper executorServiceWrapper;
    private final KafkaTopicManager kafkaTopicManager;
    private final ProducerKafka kafka_producer;
    private final EventGenerator eventGenerator;
    private final List<String> topicList;
    private final AtomicInteger queryCount;


    @Autowired
    public ProducerController(ExecutorServiceWrapper executorServiceWrapper, KafkaTopicManager kafkaTopicManager, ProducerKafka kafka_producer) {
        this.executorServiceWrapper = executorServiceWrapper;
        this.executorServiceWrapper.setThreadCount(100);
        this.kafkaTopicManager = kafkaTopicManager;
        this.kafka_producer = kafka_producer;
        eventGenerator = new EventGenerator();
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

    private void shutdown() {
        info("Initiating shutdown protocol. Killing all processes.......");
        kafkaTopicManager.deleteTopics(topicList); // This will delete all the topics which have been created
    }
}

