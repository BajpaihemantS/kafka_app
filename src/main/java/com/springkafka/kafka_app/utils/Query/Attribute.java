package com.springkafka.kafka_app.utils.Query;

public class Attribute {
    private String value;
    private Count count;
    private boolean notIncluded;

    public Attribute() {
    }

    public Attribute(String value, Count count, boolean notIncluded) {
        this.value = value;
        this.count = count;
        this.notIncluded = notIncluded;
    }

    public boolean isNotIncluded() {
        return notIncluded;
    }

    public void setNotIncluded(boolean notIncluded) {
        this.notIncluded = notIncluded;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Count getCount() {
        return count;
    }

    public void setCount(Count count) {
        this.count = count;
    }
}
