package com.springkafka.kafka_app.utils.topicAndGroupEnum;

/**
 * This enum class tells the name of the group of the consumer.
 * Currently only one group is there.
 */
public enum GroupEnum {
    GROUP("Group");
    private final String name;

    GroupEnum(String name) {
        this.name = name;
    }

    public String getGroupName() {
        return name;
    }
}