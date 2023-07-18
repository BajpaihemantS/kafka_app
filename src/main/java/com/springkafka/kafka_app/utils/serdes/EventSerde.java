package com.springkafka.kafka_app.utils.serdes;

import com.springkafka.kafka_app.event.Event;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class EventSerde implements Serde<Event> {

    private final EventSerializerDeserializer serializer;
    private final EventSerializerDeserializer deserializer;

    public EventSerde() {
        this.serializer = new EventSerializerDeserializer();
        this.deserializer = new EventSerializerDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public Serializer<Event> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Event> deserializer() {
        return deserializer;
    }
}
