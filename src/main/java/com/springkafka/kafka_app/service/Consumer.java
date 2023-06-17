package com.springkafka.kafka_app.service;

import com.springkafka.kafka_app.utils.GroupEnum;
import com.springkafka.kafka_app.utils.TopicEnum;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class Consumer {
    @KafkaListener(topics = "#{T(com.springkafka.kafka_app.utils.TopicEnum).getAllTopicNames()}", groupId = "#{T(com.springkafka.kafka_app.utils.GroupEnum).GROUP_2.getGroupName()}")
    public void readMessage(ConsumerRecord<String, String> record){
        long recordTime = record.timestamp();
        long latency = System.currentTimeMillis() - recordTime;

        System.out.println("The message recieved by consumer 1 " + record.value() + " with latency " + latency);
    }
}
