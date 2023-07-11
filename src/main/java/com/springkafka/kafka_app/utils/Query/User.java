package com.springkafka.kafka_app.utils.Query;

import org.apache.kafka.common.protocol.types.Field;

public class User {
    private String location;
    private AgeRange ageRange;

    public User() {
    }

    public User(String location, AgeRange ageRange) {
        this.location = location;
        this.ageRange = ageRange;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public AgeRange getAgeRange() {
        return ageRange;
    }

    public void setAgeRange(AgeRange ageRange) {
        this.ageRange = ageRange;
    }

}
