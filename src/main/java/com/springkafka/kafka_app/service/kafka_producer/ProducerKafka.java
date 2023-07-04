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

    private ExecutorServiceWrapper executorServiceWrapper;

//    A constructor which initialised the executor service and sets the fixed thread count
    @Autowired
    public ProducerKafka(ExecutorServiceWrapper executorServiceWrapper) {
        this.executorServiceWrapper = executorServiceWrapper;
        executorServiceWrapper.setThreadCount(ServiceProperties.MAX_PRODUCER);
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

//    This method produces a new producer with the required properties

    public Producer<String, Event> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ServiceProperties.KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, ServiceProperties.CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EventSerializerDeserializer.class);
        return new KafkaProducer<>(props);
    }

//    This method initiates the sending of event by the specified producer
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

//    A runnable function which calls for the creation of a new producer and starts sending the event
    public Runnable sendTasks(Event event){
        return () -> {
            Producer<String, Event> producer = createProducer();
            sendMessage( event, producer);
            producer.close();
        };
    }

//    This runnable function calls for the creation a new producer in a new separate thread
//    This also extracts a particular event from a list of events

    public Runnable createN_Producer(List<Event> eventList){
        return () -> {
            for(Event event : eventList){
                try {
//                    String data1 = ServiceProperties.objectmapper.writeValueAsString(data);
//                    Event event = ServiceProperties.objectmapper.readValue(data1,Event.class);
                    executorServiceWrapper.submit(sendTasks(event));

//                    executorServiceWrapper.stop();
//                    Why is this error coming

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

//    This method stops further thread from created and terminates the currently running threads

    public void shutdown(){
        executorServiceWrapper.stop();
    }

}
