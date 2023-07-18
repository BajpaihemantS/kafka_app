package com.springkafka.kafka_app.service.kafka_streams;

import com.springkafka.kafka_app.event.Event;
import com.springkafka.kafka_app.utils.Query.AgeRange;
import com.springkafka.kafka_app.utils.Query.Attribute;
import com.springkafka.kafka_app.utils.Query.AttributeType;
import com.springkafka.kafka_app.utils.Query.Query;
import com.springkafka.kafka_app.utils.ServiceProperties;
import com.springkafka.kafka_app.utils.TopicEnum;
import com.springkafka.kafka_app.utils.serdes.EventSerde;
import com.springkafka.kafka_app.utils.serdes.HashMapSerde;
import com.springkafka.kafka_app.wrapper.CustomLogger;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.stereotype.Service;


import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaStreamsService extends CustomLogger {
    private KafkaStreams kafkaStreams;
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

    public Topology streamsTopology(Query query, String topic){

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, Event> inputStream = streamsBuilder.stream(TopicEnum.TOPIC.getTopicName());

        KStream<String ,Event> timeAndEventFilterStream = inputStream
                .filter((key,event) -> {
                    boolean checkUserQuery = false;
                    if(query.getUser()==null){
                        return true;
                    }
                    Object eventUser = event.getMapKeyValue(ServiceProperties.USER_PROPERTIES);
                    if(query.getUser().getAgeRange()==null){
                        query.getUser().setAgeRange(new AgeRange(0,100));
                    }
                    if(eventUser instanceof Map){
                        Map<String, Object> userMap = (Map<String, Object>) eventUser;
                        Object eventUserLocation = userMap.getOrDefault(ServiceProperties.LOCATION, null);
                        if(eventUserLocation!=null && query.getUser().getLocation()!=null){
                            eventUserLocation = eventUserLocation.toString();
                            String queryLocation = query.getUser().getLocation();
                            checkUserQuery = eventUserLocation.equals(queryLocation);
                        }
                        int eventUserAge = Integer.parseInt(userMap.getOrDefault(ServiceProperties.AGE, 0).toString());
                        if(eventUserAge!=0 && query.getUser().getAgeRange()!=null){
                            int minAge = query.getUser().getAgeRange().getMinAge();
                            int maxAge = query.getUser().getAgeRange().getMaxAge();
                            checkUserQuery = eventUserAge >= minAge && eventUserAge <= maxAge;
                        }
                    }

                    return checkUserQuery;

                })
                .filter((key, event) -> {
                    for(AttributeType attributeType : query.getAttributeTypeList()){
                        String eventAttributeType = attributeType.getType();
                        boolean checkAttributeQuery = false;
                        for(Attribute attribute : attributeType.getAttributeList()){
                            if(attribute.getValue().equals(event.getMapKeyValue(eventAttributeType))){
                                checkAttributeQuery = true;
                            }
                        }
                        if(!checkAttributeQuery){
                            return false;
                        }
                    }
                    return true;
                });

        KTable<String, Map<String, Integer>> userAttributeCountTable = timeAndEventFilterStream
                .groupBy((key,event) -> event.getMapKeyValue(ServiceProperties.NAME).toString())
                .aggregate(
                        HashMap::new,
                        (user, event, attributeCountMap) -> {
                            for(AttributeType attributeType : query.getAttributeTypeList()) {
                                String eventAttributeType = attributeType.getType();
                                String eventAttributeValue = event.getMapKeyValue(eventAttributeType).toString();
                                Integer currentAttributeCount = attributeCountMap.getOrDefault(eventAttributeValue,0);
                                currentAttributeCount++;

                                attributeCountMap.put(eventAttributeValue,currentAttributeCount);
                            }
                            return attributeCountMap;
                        },
                        Materialized.<String, Map<String, Integer>, KeyValueStore<Bytes, byte[]>>as(
                                ServiceProperties.ATTRIBUTE_COUNT_STORE).withKeySerde(Serdes.String()).withValueSerde(new HashMapSerde()).withLoggingDisabled()

                );

        KStream<String,Map<String, Integer>> outputStream = userAttributeCountTable
                .toStream();

        outputStream.to(topic, Produced.with(Serdes.String(), new HashMapSerde()));

        return streamsBuilder.build();

    }


    public void getFilteredStream(Query query, String topic){
        Properties config = getProperties(topic);
        Topology topology = streamsTopology(query,topic);
        kafkaStreams = new KafkaStreams(topology, config);
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
