package com.springkafka.kafka_app.service.kafka_streams;


import com.springkafka.kafka_app.event.Event;
import com.springkafka.kafka_app.event.EventList;
import com.springkafka.kafka_app.utils.*;
import com.springkafka.kafka_app.utils.calculator.TimestampCalculator;
import com.springkafka.kafka_app.utils.serdes.EventSerde;
import com.springkafka.kafka_app.wrapper.CustomLogger;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.*;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class StreamsKafka extends CustomLogger {

    private KafkaStreams kafkaStreams;

    public StreamsBuilder getStreamsBuilder(){
        return new StreamsBuilder();
    }

    public void start(){
//        Properties properties = getProperties(TopicEnum.TOPIC5.getTopicName());
//
//        StreamsBuilder streamsBuilder = getStreamsBuilder();
//
//        StoreBuilder<KeyValueStore<String, EventList>> keyValueStoreStoreBuilder = Stores.keyValueStoreBuilder
//                            (Stores.inMemoryKeyValueStore(ServiceProperties.STATE_STORE),
//                            Serdes.String(),
//                            new ListEventSerde())
//                .withLoggingDisabled();
//
//        streamsBuilder.addStateStore(keyValueStoreStoreBuilder);
//
//
//        KStream<String, Event> inputStream = streamsBuilder.stream(TopicEnum.TOPIC.getTopicName());
//        inputStream
//                .mapValues(value -> {
//                    value.setMapKeyValue("timestamp",System.currentTimeMillis());
//                    return value;
//                });
////                .transform( () -> new CustomEventTransformer(), ServiceProperties.STATE_STORE);
////                .peek((key, value) -> System.out.println("vakue is " + value.getMapKeyValue("eventType")));
//
//        kafkaStreams = new KafkaStreams(streamsBuilder.build(),properties);
//
//        kafkaStreams.start();

    }


    public List<Event> getEvents(String query){
        StoreQueryParameters<ReadOnlyKeyValueStore<String, EventList>> storeQueryParameters =
                StoreQueryParameters.fromNameAndType(ServiceProperties.STATE_STORE, QueryableStoreTypes.keyValueStore());

        ReadOnlyKeyValueStore<String, EventList> stateStore = kafkaStreams.store(storeQueryParameters);

        return stateStore.get(query).getList();

    }

    public List<Event> getEvents(String query1, String query2){
        StoreQueryParameters<ReadOnlyKeyValueStore<String, EventList>> storeQueryParameters =
                StoreQueryParameters.fromNameAndType(ServiceProperties.STATE_STORE, QueryableStoreTypes.keyValueStore());

        ReadOnlyKeyValueStore<String, EventList> stateStore = kafkaStreams.store(storeQueryParameters);

        String query = query1+query2;

        return stateStore.get(query).getList();

    }

    public List<Event> getEvents(String query1, String query2, String query3){
        StoreQueryParameters<ReadOnlyKeyValueStore<String, EventList>> storeQueryParameters =
                StoreQueryParameters.fromNameAndType(ServiceProperties.STATE_STORE, QueryableStoreTypes.keyValueStore());

        ReadOnlyKeyValueStore<String, EventList> stateStore = kafkaStreams.store(storeQueryParameters);

        String query = query1+query2+query3;

        return stateStore.get(query).getList();

    }

    public List<Event> getEvents(String query, int val, char ch){
        StoreQueryParameters<ReadOnlyKeyValueStore<String, EventList>> storeQueryParameters =
                StoreQueryParameters.fromNameAndType(ServiceProperties.STATE_STORE, QueryableStoreTypes.keyValueStore());

        ReadOnlyKeyValueStore<String, EventList> stateStore = kafkaStreams.store(storeQueryParameters);

        List<Event> eventList = stateStore.get(query).getList();


        int startIndex = TimestampCalculator.getTimestampValue(eventList, val, ch);
        int endIndex = eventList.size();


        return eventList.subList(startIndex,endIndex);
    }

    public List<Event> getEvents(String query1, String query2, int val, char ch){
        StoreQueryParameters<ReadOnlyKeyValueStore<String, EventList>> storeQueryParameters =
                StoreQueryParameters.fromNameAndType(ServiceProperties.STATE_STORE, QueryableStoreTypes.keyValueStore());

        ReadOnlyKeyValueStore<String, EventList> stateStore = kafkaStreams.store(storeQueryParameters);

        String query = query1+query2;
        List<Event> eventList = stateStore.get(query).getList();


        int startIndex = TimestampCalculator.getTimestampValue(eventList, val, ch);
        int endIndex = eventList.size();


        return eventList.subList(startIndex,endIndex);

    }

    public List<Event> getEvents(String query1, String query2, String query3, int val, char ch){
        StoreQueryParameters<ReadOnlyKeyValueStore<String, EventList>> storeQueryParameters =
                StoreQueryParameters.fromNameAndType(ServiceProperties.STATE_STORE, QueryableStoreTypes.keyValueStore());

        ReadOnlyKeyValueStore<String, EventList> stateStore = kafkaStreams.store(storeQueryParameters);

        String query = query1+query2+query3;

        List<Event> eventList = stateStore.get(query).getList();

        int startIndex = TimestampCalculator.getTimestampValue(eventList, val, ch);
        int endIndex = eventList.size();

        return eventList.subList(startIndex,endIndex);
    }


//    public List<String> getRecentNames(String productId) {
//        StoreQueryParameters<ReadOnlyWindowStore<String, List<String>>> storeQueryParameters =
//                StoreQueryParameters
//                        .fromNameAndType(ServiceProperties.WISHLIST_STORE, QueryableStoreTypes.windowStore());
//
//        ReadOnlyWindowStore<String, List<String>> windowStore = kafkaStreams.store(storeQueryParameters);
//
//        Instant now = Instant.now();
//        Instant startOfWindow = now.minus(Duration.ofSeconds(30));
//        WindowStoreIterator<List<String>> iterator = windowStore.fetch(productId, startOfWindow, now);
//
//        List<String> recentNames = new ArrayList<>();
//        while (iterator.hasNext()) {
//            KeyValue<Long, List<String>> windowEntry = iterator.next();
//            List<String> names = windowEntry.value;
//            recentNames.addAll(names);
//        }
//
//        return recentNames;
//    }


    public Properties getProperties(String topic){
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, topic);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ServiceProperties.KAFKA_BROKERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, EventSerde.class);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,0);
        return properties;
    }
    public void shutdown(){
        kafkaStreams.close();
        kafkaStreams.cleanUp();
    }

}
