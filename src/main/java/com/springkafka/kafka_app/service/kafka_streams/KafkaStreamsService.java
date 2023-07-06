package com.springkafka.kafka_app.service.kafka_streams;

import com.springkafka.kafka_app.event.Event;
import com.springkafka.kafka_app.utils.Query.Attribute;
import com.springkafka.kafka_app.utils.Query.AttributeType;
import com.springkafka.kafka_app.utils.Query.Query;
import com.springkafka.kafka_app.utils.ServiceProperties;
import com.springkafka.kafka_app.utils.TopicEnum;
import com.springkafka.kafka_app.utils.serdes.HashMapSerde;
import com.springkafka.kafka_app.wrapper.CustomLogger;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class KafkaStreamsService extends CustomLogger {
    private final StreamsKafka streamsKafka;

    @Autowired
    public KafkaStreamsService(StreamsKafka streamsKafka) {
        this.streamsKafka = streamsKafka;
    }

//    public KStream<String,String> topologyBuilder(StreamsBuilder streamsBuilder, Query query, String intermediateTopic){
//        KStream<String, Event> inputStream = streamsBuilder.stream(intermediateTopic);
//
//        info("this is running after and after and ...... ");

//        KTable<String, Map<String, Integer>> userAttributeCountTable = inputStream
//                .groupBy((key,event) -> event.getMapKeyValue("name").toString())
//                .aggregate(
//                        HashMap::new,
//                        (user, event, aggregate) -> {
//                            for(AttributeType attributeType : query.getAttributeTypeList()) {
//                                String eventAttributeType = attributeType.getType();
//                                String eventAttributeValue = event.getMapKeyValue(eventAttributeType).toString();
//                                Integer currentValue = aggregate.getOrDefault(eventAttributeValue,0);
//                                currentValue++;
//                                aggregate.put(eventAttributeValue,currentValue);
//                            }
//                            return aggregate;
//                        },
//                        Materialized.<String, Map<String, Integer>, KeyValueStore<Bytes, byte[]>>as(
//                                ServiceProperties.ATTRIBUTE_COUNT_STORE).withKeySerde(Serdes.String()).withValueSerde(new HashMapSerde()).withLoggingDisabled())
//                .filter((user, attributeCount) -> {
//                    for(AttributeType attributeType : query.getAttributeTypeList()){
//                        for(Attribute attribute : attributeType.getAttributeList()){
//                            String attributeValue = attribute.getValue();
//                            Integer count = attribute.getCount();
//                            if(attributeCount.getOrDefault(attributeValue,0)<count) {
//                                return false;
//                            }
//                        }
//                    }
//                    return true;
//                });
//
//        KStream<String,String> outputStream = userAttributeCountTable
//                .toStream()
//                .peek((key,value) -> info("--------222--------",key))
//                .map((user,attributeCount) -> KeyValue.pair(user,user));
//
//        return outputStream;
//
//    }

    public void getOutputStream(Query query, String intermediateTopic, String outputTopic){

        StreamsBuilder streamsBuilder = streamsKafka.getStreamsBuilder();
        Properties config = streamsKafka.getProperties(outputTopic);

        long startTime = query.getTimestamp().getStartTime();
        long endTime = query.getTimestamp().getEndTime();

        KStream<String, Event> inputStream = streamsBuilder.stream(intermediateTopic);

        KTable<String, Map<String, Integer>> userAttributeCountTable = inputStream
                .groupBy((key,event) -> event.getMapKeyValue("name").toString())
                .aggregate(
                        HashMap::new,
                        (user, event, aggregate) -> {
                            for(AttributeType attributeType : query.getAttributeTypeList()) {
                                String eventAttributeType = attributeType.getType();
                                String eventAttributeValue = event.getMapKeyValue(eventAttributeType).toString();
                                Integer currentValue = aggregate.getOrDefault(eventAttributeValue,0);
                                currentValue++;
                                aggregate.put(eventAttributeValue,currentValue);
                            }
                            return aggregate;
                        },
                        Materialized.<String, Map<String, Integer>, KeyValueStore<Bytes, byte[]>>as(
                                ServiceProperties.ATTRIBUTE_COUNT_STORE).withKeySerde(Serdes.String()).withValueSerde(new HashMapSerde()).withLoggingDisabled())
                .filter((user, attributeCount) -> {
                    for(AttributeType attributeType : query.getAttributeTypeList()){
                        for(Attribute attribute : attributeType.getAttributeList()){
                            String attributeValue = attribute.getValue();
                            Integer count = attribute.getCount();
                            info("here the attribute count is {} ",attributeCount.getOrDefault(attributeValue,0));
                            if(attributeCount.getOrDefault(attributeValue,0)!=count) {
                                return false;
                            }
                        }
                    }
                    return true;
                });

        KStream<String,String> outputStream = userAttributeCountTable
                .toStream()
                .peek((key,value) -> info("the name of the user is {} ",key))
                .map((user,attributeCount) -> KeyValue.pair(user,user));

        outputStream.to(outputTopic, Produced.with(Serdes.String(),Serdes.String()));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), config);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdown(kafkaStreams)));

    }

//    public void getFilteredStream(Query query, String intermediateTopic, String outputTopic)  {
//        StreamsBuilder streamsBuilder = streamsKafka.getStreamsBuilder();
//        Properties config = streamsKafka.getProperties(intermediateTopic);
//
//        KStream<String, Event> inputStream = streamsBuilder.stream(TopicEnum.TOPIC.getTopicName());
//
//        KStream<String, Event> eventKStream = inputStream
//                .filter((key, event) -> {
//                    for(AttributeType attributeType : query.getAttributeTypeList()){
//                        String eventAttributeType = attributeType.getType();
//                        for(Attribute attribute : attributeType.getAttributeList()){
//                            if(attribute.getValue().equals(event.getMapKeyValue(eventAttributeType))){
//                                return true;
//                            }
//                        }
//                    }
//                    return false;
//                })
//                .peek((key,value) -> info("--------111--------",key));
//
//
//        eventKStream.to(intermediateTopic);
//
//
//        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), config);
//        kafkaStreams.start();
//
//        info("the message has reached here");
//        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdown(kafkaStreams)));
//
//    }

    public Runnable startStreams(Query query, String intermediateTopic, String outputTopic){
        return () -> getOutputStream(query,intermediateTopic,outputTopic);
    }

    public void shutdown(KafkaStreams kafkaStreams){
        info("Reached here for the first time ???!!!!!!!");
        kafkaStreams.close();
        kafkaStreams.cleanUp();
    }
}
