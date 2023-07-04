package com.springkafka.kafka_app.utils.Query;

public class Attribute {
    String value;
    Integer count;

    public Attribute() {
    }

    public Attribute(String value, Integer count) {
        this.value = value;
        this.count = count;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
}
