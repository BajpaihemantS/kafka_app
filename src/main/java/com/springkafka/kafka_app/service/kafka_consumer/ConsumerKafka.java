package com.springkafka.kafka_app.service.kafka_consumer;

import com.springkafka.kafka_app.utils.*;
import com.springkafka.kafka_app.utils.Query.Query;
import com.springkafka.kafka_app.utils.calculator.LatencyCalculator;
import com.springkafka.kafka_app.utils.calculator.QueryCheckAndPrint;
import com.springkafka.kafka_app.utils.serdes.HashMapSerializerDeserializer;
import com.springkafka.kafka_app.wrapper.CustomLogger;
import com.springkafka.kafka_app.wrapper.ExecutorServiceWrapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 *
 * This is the Consumer class with all its functionalities
 *
 */
@Service
public class ConsumerKafka extends CustomLogger {

    private final ExecutorServiceWrapper executorServiceWrapper;

    @Autowired
    public ConsumerKafka(ExecutorServiceWrapper executorServiceWrapper) {
        this.executorServiceWrapper = executorServiceWrapper;
        executorServiceWrapper.setThreadCount(ServiceProperties.MAX_CONSUMER);
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    public Consumer<String, Map<String, Long>> createConsumer(String groupId, String topic) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ServiceProperties.KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, HashMapSerializerDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, ServiceProperties.MAX_POLL_RECORDS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ServiceProperties.OFFSET_RESET_EARLIER);

        final Consumer<String, Map<String, Long>> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }


    public void runConsumer(Consumer<String, Map<String, Long>> consumer, Query query, HashSet<String> userSet) throws InterruptedException {

        int noMessageCount=1;

        while(true){

            ConsumerRecords<String, Map<String, Long>> consumerRecords = consumer.poll(1000);

            if(consumerRecords.isEmpty()){
                noMessageCount++;
                info("no message received since {} seconds", noMessageCount);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e){
                    error("Failed while trying to make consumer thread sleep with exception", e);
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
                Map<String,Long> userMap = record.value();
                info("The map is {}",userMap);
                info("---454545---");
                Long eventValue = record.value().get("sign_out");
                info("---232323--- {}", eventValue);
                info("---454545---");

//                info("the received record is {} and {}", user, record.value());

                boolean queryCheck = QueryCheckAndPrint.checkQuery(userMap,query,record.timestamp());
                info("reached here after the query check");
                boolean isUserPresent = userSet.contains(user);

                if(queryCheck && !isUserPresent){
                    userSet.add(user);
                }
                else if(!queryCheck && isUserPresent){
                    userSet.remove(user);
                }

                long latency = recordReceivedTime - record.value().get(ServiceProperties.TIMESTAMP);
                LatencyCalculator.checkAndAddLatency(latency);
            });
        }
    }

    public void printUsers(HashSet<String> userSet){
        for(String user : userSet){
            info(user);
        }
    }

    public void consumeEvents(String topic, Query query, HashSet<String> users){
        Consumer<String,Map<String, Long>> consumer = createConsumer(GroupEnum.GROUP.getGroupName(), topic);
        try {
            runConsumer(consumer, query, users);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public Runnable createN_Consumer(int n, String topic, Query query){
        return () -> {
            for(int i=0;i<n;i++){
                executorServiceWrapper.submit(() ->consumeEvents(topic,query, new HashSet<>()));
            }
        };
    }

    public void shutdown(){
        executorServiceWrapper.stop();
    }
}
