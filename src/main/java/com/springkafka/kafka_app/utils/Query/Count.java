package com.springkafka.kafka_app.utils.Query;

public class Count {
    private int value;
    private String relation;

    public Count() {
    }

    public Count(int value, String relation) {
        this.value = value;
        this.relation = relation;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public String getRelation() {
        return relation;
    }

    public void setRelation(String relation) {
        this.relation = relation;
    }
}
