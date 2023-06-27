package com.springkafka.kafka_app.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.springkafka.kafka_app.event.Event;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

class ListEventSerializerDeserializer implements Serializer<List<String>>, Deserializer<List<String>> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No additional configuration needed
    }


    @Override
    public byte[] serialize(String topic, List<String> data) {

        try {
            return ServiceProperties.objectmapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<String> deserialize(String topic, byte[] data) {
        try {
            String[] events = ServiceProperties.objectmapper.readValue(data, String[].class);
            return Arrays.asList(events);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void close() {
        // No resources to close
    }
}
