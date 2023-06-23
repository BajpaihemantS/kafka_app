package com.springkafka.kafka_app.event;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

//@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, property = "eventType")
//@JsonSubTypes({
//        @JsonSubTypes.Type(value = AddToCartEvent.class, name="addToCartEvent"),
//        @JsonSubTypes.Type(value = AddToWishlistEvent.class, name="addToWishlistEvent"),
//        @JsonSubTypes.Type(value = BuyNowEvent.class, name="buyNowEvent"),
//        @JsonSubTypes.Type(value = SignOutEvent.class, name="signOutEvent"),
//        @JsonSubTypes.Type(value = SignInEvent.class, name="signInEvent")
//})
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

//    private String eventType;
//
//    public Event() {
//    }
//
//    public Event(String eventType) {
//        this.eventType = eventType;
//    }
//
//    public String getEventType() {
//        return eventType;
//    }
//
//    public void setEventType(String eventType) {
//        this.eventType = eventType;
//    }
}
