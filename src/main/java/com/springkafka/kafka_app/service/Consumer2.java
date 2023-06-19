package com.springkafka.kafka_app.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class Consumer2 {
//    @KafkaListener(topics = "#{T(com.springkafka.kafka_app.utils.TopicEnum).getAllTopicNames()}", groupId = "#{T(com.springkafka.kafka_app.utils.GroupEnum).GROUP_1.getGroupName()}")
//    public void readMessage(ConsumerRecord<String, String> record){
//        long recordTime = record.timestamp();
//        long latency = System.currentTimeMillis() - recordTime;
//
//        System.out.println("The message recieved by 2nd consumer " + record.value() + " with latency " + latency);
//    }
}