package com.springkafka.kafka_app.utils.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class HashMapSerde implements Serde<Map<String, Long>> {

    private final Serializer<Map<String, Long>> serializer;
    private final Deserializer<Map<String, Long>> deserializer;

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
    public Serializer<Map<String, Long>> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Map<String, Long>> deserializer() {
        return deserializer;
    }
}