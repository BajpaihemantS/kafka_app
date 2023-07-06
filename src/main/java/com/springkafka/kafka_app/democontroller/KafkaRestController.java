package com.springkafka.kafka_app.democontroller;

import com.springkafka.kafka_app.config.KafkaTopicDeletion;
import com.springkafka.kafka_app.event.Event;
import com.springkafka.kafka_app.service.kafka_consumer.ConsumerKafka;
import com.springkafka.kafka_app.service.kafka_producer.ProducerKafka;
import com.springkafka.kafka_app.service.kafka_streams.KafkaStreamsService;
import com.springkafka.kafka_app.utils.*;
import com.springkafka.kafka_app.utils.Query.Query;
import com.springkafka.kafka_app.utils.calculator.LatencyCalculator;
import com.springkafka.kafka_app.wrapper.CustomLogger;
import com.springkafka.kafka_app.wrapper.ExecutorServiceWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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
    private final KafkaTopicDeletion kafkaTopicDeletion;
    private final ConsumerKafka kafka_consumer;
    private final ProducerKafka kafka_producer;
    private final EventGenerator eventGenerator;
    private final KafkaStreamsService kafkaStreamsService;

    @Autowired
    public KafkaRestController(ExecutorServiceWrapper executorServiceWrapper, KafkaTopicDeletion kafkaTopicDeletion, ConsumerKafka kafka_consumer, ProducerKafka kafka_producer, KafkaStreamsService kafkaStreamsService) {
        this.executorServiceWrapper = executorServiceWrapper;
        this.executorServiceWrapper.setThreadCount(100);
        this.kafkaTopicDeletion = kafkaTopicDeletion;
        this.kafka_consumer = kafka_consumer;
        this.kafka_producer = kafka_producer;
        eventGenerator = new EventGenerator();
        this.kafkaStreamsService = kafkaStreamsService;
        Runtime.getRuntime().addShutdownHook( new Thread(this::shutdown));
//        shellCommandExecutor = new ShellCommandExecutor();
//        shellCommandExecutor.runZookeeper();
    }

//    @GetMapping("/streams")
//    public void startStreams(){
//        streamsKafka.start();
//    }

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
        String outputTopic = TopicEnum.TOPIC2.getTopicName();
        executorServiceWrapper.submit(kafkaStreamsService.startStreams(query, outputTopic));
        executorServiceWrapper.submit(kafka_consumer.consumeEvents(outputTopic));
    }





    /**
     * @param queries in order of CustomerId, EventType and ProductId (anyone required)
     * @return the associated events
     */
//    @GetMapping("/getEvents")
//    public List<Event> getEvents(@RequestBody List<String> queries) {
//        return switch (queries.size()) {
//            case 1 -> streamsKafka.getEvents(queries.get(0));
//            case 2 -> streamsKafka.getEvents(queries.get(0), queries.get(1));
//            case 3 -> streamsKafka.getEvents(queries.get(0), queries.get(1), queries.get(2));
//            default -> Collections.emptyList();
//        };
//    }

    /**
     * @param  request in order of ->
     *                query attributes in order -> CustomerId, EventType and ProductId (anyone required)
     *                int value of how much time you want to request to be from
     *                char value denoting "S" for seconds, "H" for hours and "M" for months
     * @return the associated events
     */
//    @GetMapping("/getEvents")
//    public List<Event> getEvents(@RequestBody GetEventsRequest request) {
//        List<String> queries = request.getQueries();
//        int val = request.getIntValue();
//        char ch = request.getCharValue();
//        return switch (queries.size()) {
//            case 1 -> streamsKafka.getEvents(queries.get(0), val, ch);
//            case 2 -> streamsKafka.getEvents(queries.get(0), queries.get(1), val, ch);
//            case 3 -> streamsKafka.getEvents(queries.get(0), queries.get(1), queries.get(2), val, ch);
//            default -> Collections.emptyList();
//        };
//    }

    private void shutdown() {
        info("Initiating shutdown protocol. Killing all processes.......");
    }
}

