package com.springkafka.kafka_app.service.kafka_consumer;

import com.springkafka.kafka_app.event.Event;
import com.springkafka.kafka_app.utils.*;
import com.springkafka.kafka_app.wrapper.ExecutorServiceWrapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Properties;

@Service
public class ConsumerKafka {

    private final ExecutorServiceWrapper executorServiceWrapper;

    @Autowired
    public ConsumerKafka(ExecutorServiceWrapper executorServiceWrapper) {
        this.executorServiceWrapper = executorServiceWrapper;
        executorServiceWrapper.setThreadCount(ServiceProperties.MAX_CONSUMER);
    }

    public Consumer<String, Event> createConsumer(String groupId, String topic) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ServiceProperties.KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EventSerializerDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, ServiceProperties.MAX_POLL_RECORDS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ServiceProperties.OFFSET_RESET_LATEST);

        final Consumer<String, Event> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }


    public void runConsumer(Consumer<String, Event> consumer) throws InterruptedException {

        int noMessageCount=0;

        while(true){

            ConsumerRecords<String, Event> consumerRecords = consumer.poll(1000);

            if(consumerRecords.isEmpty()){
                noMessageCount++;
                System.out.println("the last message received before " + noMessageCount);
                if(noMessageCount > ServiceProperties.MAX_NO_MESSAGE_FOUND_COUNT) {
                    stop(consumer);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e){
                    System.out.println("Failed while trying to make consumer thread sleep with exception " + e);
                    e.printStackTrace();
                }
                continue;
            }
            else{
                noMessageCount = 0;
            }


            long recordReceivedTime = System.currentTimeMillis();
            consumerRecords.forEach(record -> {
                System.out.println("Record value is :------------------- " + record.value().getMapKeyValue("eventType"));
                System.out.println("Record topic is : " + record.topic());
                System.out.println("Record offset is : " + record.offset());
                long latency = recordReceivedTime - record.timestamp();
                System.out.println("Record latency is : " + latency);
                LatencyCalculator.checkAndAddLatency(latency);
                System.out.println();
            });
        }
    }

    public Runnable consumeEvents(){
        return () -> {
            Consumer<String,Event> consumer = createConsumer(GroupEnum.GROUP.getGroupName(),TopicEnum.TOPIC.getTopicName());
            try {
                runConsumer(consumer);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public Runnable createN_Consumer(int n){
        return () -> {
            for(int i=0;i<n;i++){
                executorServiceWrapper.submit(consumeEvents());
            }
        };
    }

    public static void stop(Consumer<String, Event> consumer){
        consumer.close();
    }
}
