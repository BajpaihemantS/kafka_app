package org.kafkaApp.utils.serdes;


import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.kafkaApp.event.Event;

public class EventSerde implements Serde<Event> {

    private final EventSerializerDeserializer serializer;
    private final EventSerializerDeserializer deserializer;

    public EventSerde() {
        this.serializer = new EventSerializerDeserializer();
        this.deserializer = new EventSerializerDeserializer();
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
