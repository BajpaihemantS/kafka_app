package com.springkafka.kafka_app.service.kafka_producer;

import com.springkafka.kafka_app.event.Event;
import com.springkafka.kafka_app.utils.EventSerializerDeserializer;
import com.springkafka.kafka_app.utils.ServiceProperties;
import com.springkafka.kafka_app.utils.TopicEnum;
import com.springkafka.kafka_app.wrapper.ExecutorServiceWrapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class Kafka_Producer {

    private static ExecutorServiceWrapper executorServiceWrapper;

    @Autowired
    public Kafka_Producer(ExecutorServiceWrapper executorServiceWrapper) {
        this.executorServiceWrapper = executorServiceWrapper;
        executorServiceWrapper.setThreadCount(ServiceProperties.MAX_PRODUCER);
    }

    public static Producer<String, Event> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ServiceProperties.KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, ServiceProperties.CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EventSerializerDeserializer.class.getName());
        return new KafkaProducer<>(props);
    }
//    Another thing we can do for topic is that we can get the message with the topic embedded in that
//    But issue is that if the producer wants to produce 1k messages it will be difficult
//    also i am thinking that rather than creating too many events we can create too many topics

    public static void sendMessage(Event event, Producer<String, Event> producer) {

        String topic = TopicEnum.TOPIC.getTopicName();
        ProducerRecord<String, Event> record = new ProducerRecord<>(topic, event);
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.println("Record sent to offset " + metadata.offset() + " and topic " + metadata.topic() + "\n");
            } else {
                System.out.println("Record submission failed");
                exception.printStackTrace();
            }
        });

    }

    public static Runnable sendTasks(Event event, int count){
        return () -> {
            Producer<String, Event> producer = createProducer();
            sendMessage( event, producer);
        };
    }

    public static Runnable createN_Producer(Event event, int count){
        return () -> {
            for(int i=0;i<count;i++){
                executorServiceWrapper.submit(sendTasks(event,count));
            }
//            executorServiceWrapper.stop();
        };
    }






//for topic we can input an integer and then use that to get the topic name




}
