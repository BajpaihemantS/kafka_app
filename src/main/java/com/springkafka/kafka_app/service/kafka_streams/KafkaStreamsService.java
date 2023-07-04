package com.springkafka.kafka_app.service.kafka_streams;

import com.springkafka.kafka_app.event.Event;
import com.springkafka.kafka_app.service.kafka_producer.ProducerKafka;
import com.springkafka.kafka_app.utils.Query.Attribute;
import com.springkafka.kafka_app.utils.Query.AttributeType;
import com.springkafka.kafka_app.utils.Query.Query;
import com.springkafka.kafka_app.utils.ServiceProperties;
import com.springkafka.kafka_app.utils.TopicEnum;
import com.springkafka.kafka_app.utils.serdes.HashMapSerde;
import com.springkafka.kafka_app.wrapper.CustomLogger;
import joptsimple.util.KeyValuePair;
import org.apache.kafka.common.protocol.types.Field;
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
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.w3c.dom.Attr;

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

    public Runnable startStreams(Query query, String topic){
        return () -> {
            StreamsBuilder streamsBuilder = streamsKafka.getStreamsBuilder();
            Properties config = streamsKafka.getProperties(topic);

//            Materialized<String, Map<String, Integer>, KeyValueStore<Bytes, byte[]>> materialized = Materialized
//                    .<String, Map<String, Integer>>as(Stores.persistentKeyValueStore(ServiceProperties.STATE_STORE))
//                    .withKeySerde(Serdes.String())
//                    .withValueSerde(new HashMapSerde())
//                    .withLoggingDisabled()
//                    .withCachingDisabled();


            long startTime = query.getTimestamp().getStartTime();
            long endTime = query.getTimestamp().getEndTime();

            KStream<String, Event> inputStream = streamsBuilder.stream(TopicEnum.TOPIC.getTopicName());
            info("reached here --0----0-0-0-0-0-0");

            KTable<String, Map<String, Integer>> userAttributeCountTable = inputStream
                    .groupBy((key,event) -> event.getMapKeyValue("name").toString())
                    .aggregate(
                            () -> new HashMap<>(),
                            (user, event, aggregate) -> {
                                for(AttributeType attributeType : query.getAttributeTypeList()) {
                                    String eventAttributeType = attributeType.getType();
                                    String eventAttributeValue = event.getMapKeyValue(eventAttributeType).toString();
                                    Integer currentValue = aggregate.getOrDefault(eventAttributeValue,0);
                                    currentValue++;

                                    aggregate.put(eventAttributeValue,currentValue);
                                }
                                info("Rechead here ------1111111");
                                return aggregate;
                            },
//                            Materialized.with(Serdes.String(),new HashMapSerde())
                            Materialized.<String, Map<String, Integer>, KeyValueStore<Bytes, byte[]>>as(
                                    ServiceProperties.STATE_STORE).withKeySerde(Serdes.String()).withValueSerde(new HashMapSerde()).withLoggingDisabled()

                    );

            KTable<String, Map<String, Integer>> filteredTable = userAttributeCountTable
                    .filter((user, attributeCount) -> {
                        info("Rechead here ------22222222");
                        for(AttributeType attributeType : query.getAttributeTypeList()){
                            info("Rechead here ------33333");
                            for(Attribute attribute : attributeType.getAttributeList()){
                                String attributeValue = attribute.getValue();
                                Integer count = attribute.getCount();
                                info("the want is for {} and count {} but gotten is count {}",attributeValue,count,attributeCount.getOrDefault(attributeValue,0));
                                if(attributeCount.getOrDefault(attributeValue,0)<count) {
                                    return false;
                                }
                                debug("reached here but i don't think this is working");
                            }
                        }
                        return true;
                    });

            KStream<String,String> outputStream = filteredTable
                    .toStream()
                    .peek((user,attribu) -> info("user name is {}",user))
                    .map((user,attributeCount) -> KeyValue.pair(user,user));

            outputStream.to(topic, Produced.with(Serdes.String(),Serdes.String()));


//            kStream
//                    .filter((key,event) -> {
//                long timestamp = Long.parseLong(event.getMapKeyValue("timestamp").toString());
//                info("attribute value is {} while attribute type is {}",attributeValue, event.getMapKeyValue(attributeType));
//
//                return attributeValue.equals(event.getMapKeyValue(attributeType))
//                        && timestamp >= startTime
//                        && timestamp <= endTime;
//
//            })
//
//                    .to(topic);

            kafkaStreams = new KafkaStreams(streamsBuilder.build(), config);
            kafkaStreams.start();

        };

    }

    public void shutdown(){
        kafkaStreams.close();
        kafkaStreams.cleanUp();
    }
}
