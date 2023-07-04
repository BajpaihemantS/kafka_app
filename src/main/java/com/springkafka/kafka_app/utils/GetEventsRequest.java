package com.springkafka.kafka_app.utils;

import java.util.List;

public class GetEventsRequest {
    private List<String> queries;
    private int intValue;
    private char charValue;

    public List<String> getQueries() {
        return queries;
    }

    public void setQueries(List<String> queries) {
        this.queries = queries;
    }

    public int getIntValue() {
        return intValue;
    }

    public void setIntValue(int intValue) {
        this.intValue = intValue;
    }

    public char getCharValue() {
        return charValue;
    }

    public void setCharValue(char charValue) {
        this.charValue = charValue;
    }
}
