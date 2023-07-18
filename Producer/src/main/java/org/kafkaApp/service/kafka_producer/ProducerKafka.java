package org.kafkaApp.service.kafka_producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.kafkaApp.event.Event;
import org.kafkaApp.utils.ServiceProperties;
import org.kafkaApp.utils.TopicEnum;
import org.kafkaApp.utils.serdes.EventSerializerDeserializer;
import org.kafkaApp.wrapper.CustomLogger;
import org.kafkaApp.wrapper.ExecutorServiceWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 *
 * This is a Producer class with all the necessary kafkaApp functions
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
        String clientId = ServiceProperties.CLIENT_ID + UUID.randomUUID().toString().split("-")[0];
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EventSerializerDeserializer.class);
        return new KafkaProducer<>(props);
    }

    public void sendEventWithProducer(Event event, Producer<String, Event> producer) {
        String topic = TopicEnum.TOPIC.getTopicName();
        ProducerRecord<String, Event> record = new ProducerRecord<>(topic, event);
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                info("Record sent to offset {} and Type is {} and {} with name {} \n", metadata.offset(), record.value().getMapKeyValue("eventType"), record.value().getMapKeyValue("productId"), record.value().getMapKeyValue("name"));
            } else {
                error("Record submission failed",exception);
                exception.printStackTrace();
            }
        });
    }

    public Runnable sendEvent(Event event){
        return () -> {
            Producer<String, Event> producer = createProducer();
            sendEventWithProducer( event, producer);
            producer.close();
        };
    }

    public Runnable createN_Producer(List<Event> eventList){
        return () -> {
            for(Event event : eventList){
                try {
                    executorServiceWrapper.submit(sendEvent(event));
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
