package com.springkafka.kafka_app.utils;

/**
 * This enum returns the topic name
 */

public enum TopicEnum {
    TOPIC("topic");

    private final String topicName;

    TopicEnum(String topicName) {
        this.topicName = topicName;
    }

    public String getTopicName() {
        return topicName;
    }

}

