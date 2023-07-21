package com.springkafka.kafka_app.utils.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.springkafka.kafka_app.utils.ServiceProperties;
import com.springkafka.kafka_app.event.Event;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class EventSerializerDeserializer implements Serializer<Event>, Deserializer<Event> {


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Event event) {
        try {
            return ServiceProperties.objectmapper.writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Event data) {
        return Serializer.super.serialize(topic, headers, data);
    }

    @Override
    public Event deserialize(String topic, byte[] data) {
        try {
            return ServiceProperties.objectmapper.readValue(data, Event.class);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Event deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
    }


}
