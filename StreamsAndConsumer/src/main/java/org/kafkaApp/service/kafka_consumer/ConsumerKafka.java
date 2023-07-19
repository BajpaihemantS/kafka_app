package org.kafkaApp.service.kafka_consumer;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.kafkaApp.utils.GroupEnum;
import org.kafkaApp.utils.QueryObject.Query;
import org.kafkaApp.utils.ServiceProperties;
import org.kafkaApp.utils.calculator.LatencyCalculator;
import org.kafkaApp.utils.calculator.QueryCheckAndPrintUsers;
import org.kafkaApp.utils.serdes.HashMapSerializerDeserializer;
import org.kafkaApp.wrapper.CustomLogger;
import org.kafkaApp.wrapper.ExecutorServiceWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 *
 * This is the Consumer class with all its functionalities
 *
 */
@Service
public class ConsumerKafka extends CustomLogger {

    private final ExecutorServiceWrapper executorServiceWrapper;
    private static final Timer consumerLatencyCalculator = Timer.builder("record_consumer_latency")
            .publishPercentiles(0.99)
            .publishPercentileHistogram()
            .register(Metrics.globalRegistry);

    @Autowired
    public ConsumerKafka(ExecutorServiceWrapper executorServiceWrapper) {
        this.executorServiceWrapper = executorServiceWrapper;
        executorServiceWrapper.setThreadCount(ServiceProperties.MAX_CONSUMER);
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    public Consumer<String, Map<String, Integer>> createConsumer(String groupId, String topic) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ServiceProperties.KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, HashMapSerializerDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, ServiceProperties.MAX_POLL_RECORDS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ServiceProperties.OFFSET_RESET_EARLIER);

        final Consumer<String, Map<String, Integer>> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }


    public void runConsumer(Consumer<String, Map<String, Integer>> consumer, Query query, HashMap<String,Long> userLatestTimeMap) {

        int noMessageCount=1;

        while(true){

            ConsumerRecords<String, Map<String, Integer>> consumerRecords = consumer.poll(Duration.ofMillis(1000));

            if(consumerRecords.isEmpty()){
                noMessageCount++;
                info("no message received since {} seconds", noMessageCount);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e){
                    error("Failed while trying to make consumer thread sleep with exception {}", e);
                    e.printStackTrace();
                }
                continue;
            }
            else{
                noMessageCount = 1;
            }


            long recordReceivedTime = System.currentTimeMillis();
            consumerRecords.forEach(record -> {
                String user = record.key();
                long userEventTime = record.timestamp();

                boolean queryCheckResult = QueryCheckAndPrintUsers.checkQuery(record.value(),query);
                boolean isUserPresent = userLatestTimeMap.containsKey(user);

                if(queryCheckResult){
                    userLatestTimeMap.put(user,userEventTime);
                }
                else if(isUserPresent){
                    userLatestTimeMap.remove(user);
                }
                long latency = recordReceivedTime - record.timestamp();
                consumerLatencyCalculator.record(latency, TimeUnit.MILLISECONDS);
//                LatencyCalculator.checkAndAddLatency(latency);
            });
        }
    }

    public void consumeEvents(String topic, Query query, HashMap<String,Long> userLatestTimeMap){
        Consumer<String,Map<String, Integer>> consumer = createConsumer(GroupEnum.GROUP.getGroupName(), topic);
        runConsumer(consumer, query, userLatestTimeMap);
    }

    public Runnable createN_Consumer(int n, String topic, Query query){
        return () -> {
            for(int i=0;i<n;i++){
                executorServiceWrapper.submit(() ->consumeEvents(topic,query, new HashMap<>()));
            }
        };
    }

    public void shutdown(){
        executorServiceWrapper.stop();
    }
}
