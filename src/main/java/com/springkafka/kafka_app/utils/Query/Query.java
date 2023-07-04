package com.springkafka.kafka_app.utils.Query;

import java.util.List;

public class Query {
    private List<AttributeType> attributeTypeList;
    private Timestamp timestamp;

    public Query() {
    }

    public Query(List<AttributeType> attributeTypeList, Timestamp timestamp) {
        this.attributeTypeList = attributeTypeList;
        this.timestamp = timestamp;
    }

    public List<AttributeType> getAttributeTypeList() {
        return attributeTypeList;
    }

    public void setAttributeTypeList(List<AttributeType> attributeTypeList) {
        this.attributeTypeList = attributeTypeList;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

}
