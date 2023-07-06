package com.springkafka.kafka_app.service.kafka_streams;

import com.springkafka.kafka_app.event.Event;
import com.springkafka.kafka_app.utils.Query.Attribute;
import com.springkafka.kafka_app.utils.Query.AttributeType;
import com.springkafka.kafka_app.utils.Query.Query;
import com.springkafka.kafka_app.utils.ServiceProperties;
import com.springkafka.kafka_app.utils.TopicEnum;
import com.springkafka.kafka_app.utils.serdes.EventSerde;
import com.springkafka.kafka_app.utils.serdes.HashMapSerde;
import com.springkafka.kafka_app.wrapper.CustomLogger;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class KafkaStreamsService extends CustomLogger {
    private KafkaStreams kafkaStreams;

    @Autowired
    public KafkaStreamsService() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    public Properties getProperties(String topic){
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, topic);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ServiceProperties.KAFKA_BROKERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, EventSerde.class);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,0);
        return properties;
    }

    public void getFilteredStream(Query query, String topic){

//        if(kafkaStreams!=null){
//            kafkaStreams.close();
//            kafkaStreams.cleanUp();
//            info("the stream has stopped and output topic has been deleted");
//        }

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        Properties config = getProperties(topic);

        long startTime = query.getTimestamp().getStartTime();
        long endTime = query.getTimestamp().getEndTime();

        KStream<String, Event> inputStream = streamsBuilder.stream(TopicEnum.TOPIC.getTopicName());


        KStream<String ,Event> timeAndEventFilterStream = inputStream
                .filter((key,event) -> {
                    info("this is the result of the query ");
                    long eventTime = Long.parseLong(event.getMapKeyValue("timestamp").toString());
                    return eventTime>=startTime && eventTime<=endTime;
                })
                .filter((key, event) -> {
                    for(AttributeType attributeType : query.getAttributeTypeList()){
                        String eventAttributeType = attributeType.getType();
                        for(Attribute attribute : attributeType.getAttributeList()){
                            if(attribute.getValue().equals(event.getMapKeyValue(eventAttributeType))){
                                return true;
                            }
                        }
                    }
                    return false;
                });

        KTable<String, Map<String, Integer>> userAttributeCountTable = timeAndEventFilterStream
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
                                ServiceProperties.ATTRIBUTE_COUNT_STORE).withKeySerde(Serdes.String()).withValueSerde(new HashMapSerde()).withLoggingDisabled()

                );

/**
 *  the below is the filtering part where the filtering was happening in kafka streams itself
 *  but this was not selected due to the error
 */
//                .filter((user, attributeCount) -> {
//                    info("--------555--------");
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

        KStream<String,Map<String, Integer>> outputStream = userAttributeCountTable
                .toStream();

        outputStream.to(topic, Produced.with(Serdes.String(), new HashMapSerde()));

        kafkaStreams = new KafkaStreams(streamsBuilder.build(), config);
        kafkaStreams.start();

    }

    public Runnable startStreams(Query query, String topic){
        return () -> getFilteredStream(query,topic);
    }

    public void shutdown(){
        if(kafkaStreams!=null){
            kafkaStreams.close();
            kafkaStreams.cleanUp();
        }
    }
}
