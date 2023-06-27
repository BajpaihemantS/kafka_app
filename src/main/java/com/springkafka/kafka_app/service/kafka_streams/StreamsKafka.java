package com.springkafka.kafka_app.service.kafka_streams;


import com.springkafka.kafka_app.event.Event;
import com.springkafka.kafka_app.utils.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.*;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

@Service
public class StreamsKafka extends CustomLogger {

    private KafkaStreams kafkaStreams;
    public void start(){
        Properties properties = getProperties();

        StreamsBuilder streamsBuilder = new StreamsBuilder();
//        streamsBuilder.addStateStore(
//                Stores.keyValueStoreBuilder(
//                        Stores.persistentKeyValueStore(ServiceProperties.WISHLIST_STORE),
//                        Serdes.String(),
//                        Serdes.String()
//                ).withLoggingEnabled(new HashMap<>())
//        );

        StoreBuilder<KeyValueStore<String,String >> keyValueStoreStoreBuilder = Stores.keyValueStoreBuilder
                            (Stores.inMemoryKeyValueStore(ServiceProperties.WISHLIST_STORE),
                            Serdes.String(),
                            Serdes.String())
                .withLoggingDisabled();

        streamsBuilder.addStateStore(keyValueStoreStoreBuilder);


        KStream<String, Event> inputStream = streamsBuilder.stream(TopicEnum.TOPIC.getTopicName());
        inputStream
                .filter( (key,event) -> "add_to_Wishlist".equals(event.getMapKeyValue("eventType")) )
                .peek((key, value) -> System.out.println("vakue is " + value.getMapKeyValue("name")))
                .transform(() -> new CustomTransformer(), ServiceProperties.WISHLIST_STORE)
                .peek((key, value) -> System.out.println("vakue is " + value.getMapKeyValue("name")));


        kafkaStreams = new KafkaStreams(streamsBuilder.build(),properties);

        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
//
//    public void processStateStore(String productId, String name){
//        System.out.println("----111-----");
//        KeyValueStore<String,List<String>> productIdNameStore = (KeyValueStore<String,List<String>>) getQueryStore(ServiceProperties.WISHLIST_STORE);
//        System.out.println("----444---");
//        List<String> nameList = productIdNameStore.get(productId);
//        if(nameList==null) nameList = new ArrayList<>();
//        nameList.add(name);
//        productIdNameStore.put(productId,nameList);
//    }

//    public ReadOnlyKeyValueStore<String,List<String>> getQueryStore(String storeName){
//        System.out.println("----222---");
//        StoreQueryParameters<ReadOnlyKeyValueStore<String,List<String>>> storeQueryParameters = StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore());
//        System.out.println("----333---");
//        ReadOnlyKeyValueStore<String,List<String>> store = kafkaStreams.store(storeQueryParameters);
//        System.out.println("----3.53.53.5---");
//        return store;
//    }

    public String getNames(String productId){
        StoreQueryParameters<ReadOnlyKeyValueStore<Object, Object>> storeQueryParameters =
                StoreQueryParameters.fromNameAndType(ServiceProperties.WISHLIST_STORE, QueryableStoreTypes.keyValueStore());

        ReadOnlyKeyValueStore<Object, Object> stateStore = kafkaStreams.store(storeQueryParameters);

        System.out.println(productId);
        System.out.println(stateStore.all());

        return stateStore.get(productId).toString();

    }

    public Properties getProperties(){
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wishlist-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ServiceProperties.KAFKA_BROKERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, EventSerde.class);

        return properties;
    }

}