package com.springkafka.kafka_app.service;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

@Service
public class ProducerRunner {

    public static void runProducer(String topic, String message) throws Exception {
        Producer<String,String> producer = ProducerCreator.createProducer();
        ProducerRecord<String, String> record = new ProducerRecord<>(topic,message);

        try {
            RecordMetadata metadata = producer.send(record).get();
            System.out.println("Record sent to topic " + metadata.topic() + " with offset " + metadata.offset());
        } catch (Exception e){
            System.out.println("Exception returned " + e);
            e.printStackTrace();
        }
    }



}
