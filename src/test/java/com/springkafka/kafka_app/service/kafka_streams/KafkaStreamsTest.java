package com.springkafka.kafka_app.service.kafka_streams;

import com.springkafka.kafka_app.event.Event;
import com.springkafka.kafka_app.utils.EventGenerator;
import com.springkafka.kafka_app.utils.Query.*;
import com.springkafka.kafka_app.utils.ServiceProperties;
import com.springkafka.kafka_app.utils.serdes.EventSerde;
import com.springkafka.kafka_app.utils.serdes.EventSerializerDeserializer;
import com.springkafka.kafka_app.utils.serdes.HashMapSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.mockito.Mockito.mock;

public class KafkaStreamsTest {
    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String, Event> inputTopic;
    private TestOutputTopic<String, Map<String, Integer>> outputTopic;

    private Topology topology;
    private final Properties config;


    public KafkaStreamsTest(){
        config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, ServiceProperties.CLIENT_ID);
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ServiceProperties.KAFKA_BROKERS);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, EventSerde.class);
    }

    @AfterEach
    public void shutdown(){
        topologyTestDriver.close();
    }

    @Test
    public void testGetFilteredStream() {

        KafkaStreamsService kafkaStreamsService = new KafkaStreamsService();

        Query query = new Query();
        Attribute attribute1 = new Attribute("buy_now", new Count(2, "lt"));
        Attribute attribute2 = new Attribute("sign_out", new Count(2, "lt"));
        AttributeType attributeType = new AttributeType("eventType", Arrays.asList(attribute1,attribute2));
        Timestamp timestamp = new Timestamp(0L,2088218478702L);
        query.setAttributeTypeList(Collections.singletonList(attributeType));
        query.setTimestamp(timestamp);

        String inputTopicName = "topic";
        String outputTopicName = "outputTopic";

        topology = kafkaStreamsService.streamsTopology(query,outputTopicName);
        topologyTestDriver = new TopologyTestDriver(topology,config);

        inputTopic = topologyTestDriver.createInputTopic(inputTopicName, Serdes.String().serializer(), new EventSerializerDeserializer());
        outputTopic = topologyTestDriver.createOutputTopic(outputTopicName, Serdes.String().deserializer(), new HashMapSerde().deserializer());

        EventGenerator eventGenerator = new EventGenerator();
        Event event = eventGenerator.generateEvent();
        String userName = event.getMapKeyValue("name").toString();

        inputTopic.pipeInput(event);

        Assertions.assertEquals(userName,outputTopic.readRecord().key());

    }


}
