package com.springkafka.kafka_app.service.kafka_producer;

import com.springkafka.kafka_app.event.Event;
import com.springkafka.kafka_app.wrapper.CustomLogger;
import com.springkafka.kafka_app.utils.serdes.EventSerializerDeserializer;
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

import java.util.List;
import java.util.Properties;

/**
 *
 * This is a Producer class with all the necessary producer functions
 *
 */

@Service
public class ProducerKafka extends CustomLogger {

    private final ExecutorServiceWrapper executorServiceWrapper;

    @Autowired
    public ProducerKafka(ExecutorServiceWrapper executorServiceWrapper) {
        this.executorServiceWrapper = executorServiceWrapper;
        executorServiceWrapper.setThreadCount(ServiceProperties.MAX_PRODUCER);
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    public Producer<String, Event> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ServiceProperties.KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, ServiceProperties.CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EventSerializerDeserializer.class);
        return new KafkaProducer<>(props);
    }

    public void sendMessage(Event event, Producer<String, Event> producer) {
        String topic = TopicEnum.TOPIC.getTopicName();
        ProducerRecord<String, Event> record = new ProducerRecord<>(topic, event);
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                info("Record sent to offset {} and Type is {} \n", metadata.offset(), record.value().getMapKeyValue("eventType"));
            } else {
                error("Record submission failed",exception);
                exception.printStackTrace();
            }
        });
    }

    public Runnable sendTasks(Event event){
        return () -> {
            Producer<String, Event> producer = createProducer();
            sendMessage( event, producer);
            producer.close();
        };
    }

    public Runnable createN_Producer(List<Event> eventList){
        return () -> {
            for(Event event : eventList){
                try {
                    executorServiceWrapper.submit(sendTasks(event));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public void shutdown(){
        executorServiceWrapper.stop();
    }

}
