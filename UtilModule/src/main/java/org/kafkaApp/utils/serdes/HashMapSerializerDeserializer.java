package org.kafkaApp.utils.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.kafkaApp.utils.ServiceProperties;

import java.util.HashMap;
import java.util.Map;

public class HashMapSerializerDeserializer implements Serializer<Map<String, Integer>>, Deserializer<Map<String, Integer>> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Map<String, Integer> data) {

        try {
            return ServiceProperties.objectmapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public HashMap<String, Integer> deserialize(String topic, byte[] data) {
        try {
            return (HashMap<String, Integer>)ServiceProperties.objectmapper.readValue(data,HashMap.class);

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
