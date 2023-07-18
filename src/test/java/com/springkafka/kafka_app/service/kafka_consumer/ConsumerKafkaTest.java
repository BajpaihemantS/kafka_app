package com.springkafka.kafka_app.service.kafka_consumer;

import com.springkafka.kafka_app.utils.Query.*;
import com.springkafka.kafka_app.wrapper.ExecutorServiceWrapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.mockito.Mockito.*;

class ConsumerKafkaTest {

    @Test
    void testHandleRecords() {

        ExecutorServiceWrapper executorServiceWrapper = mock(ExecutorServiceWrapper.class);
        ConsumerKafka consumerKafka = new ConsumerKafka(executorServiceWrapper);

        Query query = new Query();
        Attribute attribute1 = new Attribute("add_to_cart", new Count(2, "lt"));
        Attribute attribute2 = new Attribute("sign_out", new Count(2, "lt"));
        AttributeType attributeType = new AttributeType("eventType",Arrays.asList(attribute1,attribute2));
        Timestamp timestamp = new Timestamp(0L,2088218478702L);
        query.setAttributeTypeList(Collections.singletonList(attributeType));
        query.setTimestamp(timestamp);

        HashMap<String, Long> userLatestTimeMap = new HashMap<>();

        ConsumerRecord<String, Map<String, Integer>> record1 = new ConsumerRecord<>("topic", 0, 0L, "user1", createEventData());
        ConsumerRecords<String, Map<String, Integer>> consumerRecords = new ConsumerRecords<>(Map.of(new TopicPartition("topic",0), List.of(record1)));

        consumerKafka.handleRecords(consumerRecords,query,userLatestTimeMap);

        Assertions.assertTrue(userLatestTimeMap.containsKey("user1"));
        Assertions.assertFalse(userLatestTimeMap.containsKey("user2"));
    }

    private Map<String, Integer> createEventData() {
        Map<String, Integer> eventData = new HashMap<>();
        eventData.put("add_to_cart", 1);
        eventData.put("sign_out", 1);
        return eventData;
    }



}
