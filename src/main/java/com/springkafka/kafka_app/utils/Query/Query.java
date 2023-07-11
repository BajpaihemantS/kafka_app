package com.springkafka.kafka_app.utils.Query;

import java.util.List;

public class Query {
    private User user;
    private List<AttributeType> attributeTypeList;
    private Timestamp timestamp;


    public Query() {
    }


    public Query(User user, List<AttributeType> attributeTypeList, Timestamp timestamp) {
        this.user = user;
        this.attributeTypeList = attributeTypeList;
        this.timestamp = timestamp;
    }
    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
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
