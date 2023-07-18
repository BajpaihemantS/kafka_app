package org.kafkaApp.utils.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class HashMapSerde implements Serde<Map<String, Integer>> {

    private final Serializer<Map<String,Integer >> serializer;
    private final Deserializer<Map<String, Integer>> deserializer;

    public HashMapSerde() {
        this.serializer = new HashMapSerializerDeserializer();
        this.deserializer = new HashMapSerializerDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {
        // No resources to close
    }

    @Override
    public Serializer<Map<String, Integer>> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Map<String, Integer>> deserializer() {
        return deserializer;
    }
}