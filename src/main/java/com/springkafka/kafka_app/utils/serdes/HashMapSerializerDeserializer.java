package com.springkafka.kafka_app.utils.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.springkafka.kafka_app.utils.ServiceProperties;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

public class HashMapSerializerDeserializer implements Serializer<Map<String, Long>>, Deserializer<Map<String, Long>> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Map<String, Long> data) {

        try {
            return ServiceProperties.objectmapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public HashMap<String, Long> deserialize(String topic, byte[] data) {
        try {
            return (HashMap<String, Long>)ServiceProperties.objectmapper.readValue(data,HashMap.class);

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
