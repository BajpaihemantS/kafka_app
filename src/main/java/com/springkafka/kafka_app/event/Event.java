package com.springkafka.kafka_app.event;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, property = "eventType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = AddToCartEvent.class, name="addToCartEvent"),
        @JsonSubTypes.Type(value = AddToWishlistEvent.class, name="addToWishlistEvent"),
        @JsonSubTypes.Type(value = BuyNowEvent.class, name="buyNowEvent"),
        @JsonSubTypes.Type(value = SignOutEvent.class, name="signOutEvent"),
        @JsonSubTypes.Type(value = SignInEvent.class, name="signInEvent")
})
public abstract class Event {
    private String eventType;

    public Event() {
    }

    public Event(String eventType) {
        this.eventType = eventType;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }
}
