package com.springkafka.kafka_app.utils.Query;

import java.util.List;

public class AttributeType {
    private String type;
    private List<Attribute> attributeList;

    public AttributeType() {
    }

    public AttributeType(String type, List<Attribute> attributeList) {
        this.type = type;
        this.attributeList = attributeList;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<Attribute> getAttributeList() {
        return attributeList;
    }

    public void setAttributeList(List<Attribute> attributeList) {
        this.attributeList = attributeList;
    }
}
