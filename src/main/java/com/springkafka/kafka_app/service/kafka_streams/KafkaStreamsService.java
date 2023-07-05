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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Service
public class KafkaStreamsService extends CustomLogger {
    private final StreamsKafka streamsKafka;
    private KafkaStreams kafkaStreams;

    @Autowired
    public KafkaStreamsService(StreamsKafka streamsKafka) {
        this.streamsKafka = streamsKafka;
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }



    public void getFilteredStream(Query query, String topic){
        StreamsBuilder streamsBuilder = streamsKafka.getStreamsBuilder();
        Properties config = streamsKafka.getProperties(topic);

//        StoreBuilder<KeyValueStore<String, Long>> keyValueStoreStoreBuilder = Stores.keyValueStoreBuilder
//                        (Stores.inMemoryKeyValueStore(ServiceProperties.USER_COUNT_STORE),
//                                Serdes.String(),
//                                Serdes.Long())
//                .withLoggingDisabled();
//
//        streamsBuilder.addStateStore(keyValueStoreStoreBuilder);

//        long startTime = query.getTimestamp().getStartTime();
//        long endTime = query.getTimestamp().getEndTime();

        KStream<String, Event> inputStream = streamsBuilder.stream(TopicEnum.TOPIC.getTopicName());

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
                                ServiceProperties.ATTRIBUTE_COUNT_STORE).withKeySerde(Serdes.String()).withValueSerde(new HashMapSerde()).withLoggingDisabled()

                )
                .filter((user, attributeCount) -> {
                    for(AttributeType attributeType : query.getAttributeTypeList()){
                        for(Attribute attribute : attributeType.getAttributeList()){
                            String attributeValue = attribute.getValue();
                            Integer count = attribute.getCount();
                            if(attributeCount.getOrDefault(attributeValue,0)<count) {
                                return false;
                            }
                        }
                    }
                    return true;
                });


        KStream<String,String> outputStream = userAttributeCountTable
                .toStream()
                .map((user,attributeCount) -> KeyValue.pair(user,user));

        outputStream.to(topic, Produced.with(Serdes.String(),Serdes.String()));

        kafkaStreams = new KafkaStreams(streamsBuilder.build(), config);
        kafkaStreams.start();

    }

    public Runnable startStreams(Query query, String topic){
        return () -> getFilteredStream(query,topic);
    }

    public void shutdown(){
        kafkaStreams.close();
        kafkaStreams.cleanUp();
    }
}
