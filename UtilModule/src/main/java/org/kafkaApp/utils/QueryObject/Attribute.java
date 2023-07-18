package org.kafkaApp.utils.QueryObject;

public class Attribute {
    String value;
    Count count;

    public Attribute() {
    }

    public Attribute(String value, Count count) {
        this.value = value;
        this.count = count;
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
