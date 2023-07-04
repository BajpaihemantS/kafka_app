package com.springkafka.kafka_app.service.kafka_consumer;

import com.springkafka.kafka_app.event.Event;
import com.springkafka.kafka_app.utils.*;
import com.springkafka.kafka_app.utils.calculator.LatencyCalculator;
import com.springkafka.kafka_app.utils.serdes.EventSerializerDeserializer;
import com.springkafka.kafka_app.wrapper.CustomLogger;
import com.springkafka.kafka_app.wrapper.ExecutorServiceWrapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Properties;

/**
 *
 * This is the Consumer class with all its functionalities
 *
 */
@Service
public class ConsumerKafka extends CustomLogger {

    private final ExecutorServiceWrapper executorServiceWrapper;

//    A constructor which initialised the executor service and sets the fixed thread count
    @Autowired
    public ConsumerKafka(ExecutorServiceWrapper executorServiceWrapper) {
        this.executorServiceWrapper = executorServiceWrapper;
        executorServiceWrapper.setThreadCount(ServiceProperties.MAX_CONSUMER);
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

//    This method produces a new consumer with the required properties

    public Consumer<String, String> createConsumer(String groupId, String topic) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ServiceProperties.KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, ServiceProperties.MAX_POLL_RECORDS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ServiceProperties.OFFSET_RESET_EARLIER);

        final Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

//    This method initiates the consuming of event by the specified consumer

    public void runConsumer(Consumer<String, String> consumer) throws InterruptedException {

        int noMessageCount=1;

        while(true){

            ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);

            if(consumerRecords.isEmpty()){
                noMessageCount++;
                info("no message received since {} seconds", noMessageCount);
                if(noMessageCount > ServiceProperties.MAX_NO_MESSAGE_FOUND_COUNT) {
                    stop(consumer);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e){
                    error("Failed while trying to make consumer thread sleep with exception", e);
                    e.printStackTrace();
                }
                continue;
            }
            else{
                noMessageCount = 1;
            }

//            Printing the required received record values


            long recordReceivedTime = System.currentTimeMillis();
            consumerRecords.forEach(record -> {
//                info("Record value type is : {}", record.value().getMapKeyValue("timestamp"));
                info("Record value type is : {}", record.key().toString());
//                info("Record offset is : {}", record.offset());
                long latency = recordReceivedTime - record.timestamp();
//                info("Record latency is : {}", + latency);
                LatencyCalculator.checkAndAddLatency(latency);
            });
        }
    }

//    A runnable function which calls for the creation of a new consumer and starts consuming the message


    public Runnable consumeEvents(String topic){
        return () -> {
            Consumer<String,String> consumer = createConsumer(GroupEnum.GROUP.getGroupName(), topic);
            try {
                runConsumer(consumer);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
    }

//    This runnable function calls for the creation a new consumer in a new separate thread

    public Runnable createN_Consumer(int n, String topic){
        return () -> {
            for(int i=0;i<n;i++){
                executorServiceWrapper.submit(consumeEvents(topic));
            }
        };
    }

//    A method to stop the consumer

    public static void stop(Consumer<String, String> consumer){
        consumer.close();
    }

//    This method stops further thread from created and terminates the currently running threads

    public void shutdown(){
        executorServiceWrapper.stop();
    }
}
