package com.springkafka.kafka_app.democontroller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.springkafka.kafka_app.config.KafkaTopicDeletion;
import com.springkafka.kafka_app.config.ShellCommandExecutor;
import com.springkafka.kafka_app.event.Event;
import com.springkafka.kafka_app.service.kafka_consumer.ConsumerKafka;
import com.springkafka.kafka_app.service.kafka_producer.ProducerKafka;
import com.springkafka.kafka_app.service.kafka_streams.StreamsKafka;
import com.springkafka.kafka_app.utils.CustomLogger;
import com.springkafka.kafka_app.utils.EventGenerator;
import com.springkafka.kafka_app.utils.LatencyCalculator;
import com.springkafka.kafka_app.utils.ServiceProperties;
import com.springkafka.kafka_app.wrapper.ExecutorServiceWrapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
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
public class KafkaRestController extends CustomLogger  {
    private final ExecutorServiceWrapper executorServiceWrapper;
    private final KafkaTopicDeletion kafkaTopicDeletion;
    private final ConsumerKafka kafka_consumer;
    private final ProducerKafka kafka_producer;
    private final EventGenerator eventGenerator;

    private final StreamsKafka streamsKafka;

    @Autowired
    public KafkaRestController(ExecutorServiceWrapper executorServiceWrapper, KafkaTopicDeletion kafkaTopicDeletion, ConsumerKafka kafka_consumer, ProducerKafka kafka_producer, StreamsKafka streamsKafka) {
        this.executorServiceWrapper = executorServiceWrapper;
        this.executorServiceWrapper.setThreadCount(3);
        this.kafkaTopicDeletion = kafkaTopicDeletion;
        this.kafka_consumer = kafka_consumer;
        this.kafka_producer = kafka_producer;
        eventGenerator = new EventGenerator();
        this.streamsKafka = streamsKafka;
        Runtime.getRuntime().addShutdownHook( new Thread(this::shutdown));
//        shellCommandExecutor = new ShellCommandExecutor();
//        shellCommandExecutor.runZookeeper();
    }

    @GetMapping("/streams")
    public void startStreams(@RequestBody String productId){
        streamsKafka.start(productId);
    }

    @GetMapping("/producer")
    public void produceEvents() {
        List<Event> eventList = eventGenerator.generateNEvents(ServiceProperties.MAX_EVENTS);
        executorServiceWrapper.submit(kafka_producer.createN_Producer(eventList));
    }

    @GetMapping("/consumer")
    public void consumeEvents() {
        executorServiceWrapper.submit(kafka_consumer.consumeEvents());
    }

    @GetMapping("/stats")
    public String getLatencyStats(){
        return LatencyCalculator.printStats();
    }

    private void shutdown() {
        info("Initiating shutdown protocol. Killing all processes.......");
        Runtime.getRuntime().addShutdownHook(new Thread(kafka_consumer::shutdown));
        Runtime.getRuntime().addShutdownHook(new Thread(kafka_producer::shutdown));
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaTopicDeletion::stop));

    }
}
