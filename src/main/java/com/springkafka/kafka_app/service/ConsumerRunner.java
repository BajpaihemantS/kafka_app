package com.springkafka.kafka_app.service;

import com.springkafka.kafka_app.utils.GroupEnum;
import com.springkafka.kafka_app.utils.ServiceProperties;
import com.springkafka.kafka_app.utils.TopicEnum;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.stereotype.Service;

import java.time.Duration;

import static java.lang.System.currentTimeMillis;

@Service
public class ConsumerRunner {
//    @KafkaListener(topics = "#{T(com.springkafka.kafka_app.utils.TopicEnum).getAllTopicNames()}", groupId = "#{T(com.springkafka.kafka_app.utils.GroupEnum).GROUP_2.getGroupName()}")
//    public void readMessage(ConsumerRecord<String, String> record){
//        long recordTime = record.timestamp();
//        long latency = System.currentTimeMillis() - recordTime;
//
//        System.out.println("The message recieved by consumer 1 " + record.value() + " with latency " + latency);
//    }

    public static void runConsumer() {

        Consumer<String, String> consumer = ConsumerCreator.createConsumer(GroupEnum.GROUP_3.getGroupName(), TopicEnum.TOPIC1.getTopicName());
        int noMessageFound = 0;

        while(true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);

//            if (consumerRecords.count() == 0) {
//                noMessageFound++;
//                if (noMessageFound > ServiceProperties.MAX_NO_MESSAGE_FOUND_COUNT) {
//                    System.out.println("no message found for 100 times");
//                    break;
//                }
//                else
//                    continue;
//            }

            consumerRecords.forEach(record -> {
                System.out.println("Record value is : " + record.value());
                System.out.println("Record topic is : " + record.topic());
                System.out.println("Record offset is : " + record.offset());
                System.out.println("Record latency is : " + (System.currentTimeMillis() - record.timestamp()));

            });

        }
    }





}
