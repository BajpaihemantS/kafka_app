package org.kafkaApp.service.kafka_streams;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
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
import org.kafkaApp.event.Event;
import org.kafkaApp.utils.QueryObject.AgeRange;
import org.kafkaApp.utils.QueryObject.Attribute;
import org.kafkaApp.utils.QueryObject.AttributeType;
import org.kafkaApp.utils.QueryObject.Query;
import org.kafkaApp.utils.ServiceProperties;
import org.kafkaApp.utils.TopicEnum;
import org.kafkaApp.utils.serdes.EventSerde;
import org.kafkaApp.utils.serdes.HashMapSerde;
import org.kafkaApp.wrapper.CustomLogger;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaStreamsService extends CustomLogger {
    private KafkaStreams kafkaStreams;
    private static final  Timer streamsLatencyCalculator = Timer.builder("record_stream_latency")
            .register(Metrics.globalRegistry);

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

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        Properties config = getProperties(topic);

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
                        boolean checkAttributeQuery = false; // This variable checks for all attributeType and returns true if even one satisfies
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
                .groupBy((key,event) -> event.getMapKeyValue(ServiceProperties.NAME).toString()) // Making the username as key
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
                            long streamsProcessingLatency = System.currentTimeMillis() - (Long)(event.getMapKeyValue(ServiceProperties.TIMESTAMP));
                            streamsLatencyCalculator.record(streamsProcessingLatency, TimeUnit.MILLISECONDS);
                            return attributeCountMap;
                        },
                        Materialized.<String, Map<String, Integer>, KeyValueStore<Bytes, byte[]>>as(
                                ServiceProperties.ATTRIBUTE_COUNT_STORE).withKeySerde(Serdes.String()).withValueSerde(new HashMapSerde()).withLoggingDisabled()

                );

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
