package com.springkafka.kafka_app.service.kafka_streams;


import com.springkafka.kafka_app.event.Event;
import com.springkafka.kafka_app.utils.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class StreamsKafka extends CustomLogger {

    public void start(String ProductId){
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wishlist-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ServiceProperties.KAFKA_BROKERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, EventSerde.class);

        info("The streams has started");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, Event> inputTopic = streamsBuilder.stream(TopicEnum.TOPIC.getTopicName());
        KStream<String, Event> processedStream  = inputTopic
                .filter( (key,event) -> "add_to_Wishlist".equals(event.getMapKeyValue("eventType")) && ProductId.equals(event.getMapKeyValue("productId")) )
                .peek( (key,event) -> info("the name of the custumber is {} \n", event.getMapKeyValue("name") ) );


        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(),properties);
        info("The streams has started");

        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        info("The streams has ended");
    }
}