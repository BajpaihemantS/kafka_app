package com.springkafka.kafka_app.event;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * This is the class where the coming Event is being mapped.
 * This is designed for handling the dynamic data and maps
 * it to a map of String and Object
 *
 */

public class Event {

    private Map<String,Object> mapKeyValue = new LinkedHashMap<>();

    public Event() {
    }

    public Map<String, Object> getMapKeyValue() {
        return mapKeyValue;
    }

    public void setMapKeyValue(Map<String, Object> mapKeyValue) {
        this.mapKeyValue = mapKeyValue;
    }

    @JsonAnyGetter
    public Object getMapKeyValue(String key) {
        return mapKeyValue.get(key);
    }

    @JsonAnySetter
    public void setMapKeyValue(String key, Object value) {
        mapKeyValue.put(key,value);
    }

}
