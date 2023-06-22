package com.springkafka.kafka_app.service.kafka_consumer;

import com.springkafka.kafka_app.event.Event;
import com.springkafka.kafka_app.utils.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class Kafka_Consumer {

    public static Consumer<String, Event> createConsumer(String groupId, String topic) {
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


    public static void runConsumer(Consumer<String, Event> consumer) throws InterruptedException {

        int noMessageCount=0;

        while(true){

            ConsumerRecords<String, Event> consumerRecords = consumer.poll(1000);

            if(consumerRecords.isEmpty()){
                noMessageCount++;
                if(noMessageCount > ServiceProperties.MAX_NO_MESSAGE_FOUND_COUNT) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e){
                        System.out.println("Failed with exception " + e);
                        e.printStackTrace();
                    }
                }
                continue;
            }


            long recordReceivedTime = System.currentTimeMillis();
            consumerRecords.forEach(record -> {
                System.out.println("Record value is : " + record.value());
                System.out.println("Record topic is : " + record.topic());
                System.out.println("Record offset is : " + record.offset());
                long latency = recordReceivedTime - record.timestamp();
                System.out.println("Record latency is : " + latency);
                LatencyCalculator.checkAndAddLatency(latency);
                System.out.println();
            });
        }
    }

    public static Runnable createN_Consumer(int n){

        return () -> {
            for(int i=0;i<n;i++){
                Consumer<String,Event> consumer = createConsumer(GroupEnum.GROUP.getGroupName(),TopicEnum.TOPIC.getTopicName());
                runConsumer(consumer);
            }
        };

    }

    public static void stop(Consumer<String, String> consumer){
        consumer.close();
    }
}
