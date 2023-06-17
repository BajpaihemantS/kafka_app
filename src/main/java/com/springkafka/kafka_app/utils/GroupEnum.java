package com.springkafka.kafka_app.utils;

public enum GroupEnum {
    GROUP_1("Group 1"),
    GROUP_2("Group 2"),
    GROUP_3("Group 3");

    private final String name;

    GroupEnum(String name) {
        this.name = name;
    }

    public String getGroupName() {
        return name;
    }
}