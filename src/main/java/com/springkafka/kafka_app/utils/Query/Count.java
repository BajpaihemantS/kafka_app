package com.springkafka.kafka_app.utils.Query;

public class Count {
    private long value;
    private String relation;

    public Count() {
    }

    public Count(long value, String relation) {
        this.value = value;
        this.relation = relation;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public String getRelation() {
        return relation;
    }

    public void setRelation(String relation) {
        this.relation = relation;
    }
}
