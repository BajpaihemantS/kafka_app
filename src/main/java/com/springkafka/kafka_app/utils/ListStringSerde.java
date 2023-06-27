package com.springkafka.kafka_app.utils;

import com.springkafka.kafka_app.event.Event;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.List;
import java.util.Map;

public class ListStringSerde implements Serde<List<String>> {

    private final Serializer<List<String>> serializer;
    private final Deserializer<List<String>> deserializer;

    public ListStringSerde() {
        this.serializer = new ListEventSerializerDeserializer();
        this.deserializer = new ListEventSerializerDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {
        // No resources to close
    }

    @Override
    public Serializer<List<String>> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<List<String>> deserializer() {
        return deserializer;
    }
}