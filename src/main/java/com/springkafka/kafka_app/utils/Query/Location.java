package com.springkafka.kafka_app.utils.Query;

public class Location {
    private String city;

    public Location() {
    }

    public Location(String city) {
        this.city = city;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}
