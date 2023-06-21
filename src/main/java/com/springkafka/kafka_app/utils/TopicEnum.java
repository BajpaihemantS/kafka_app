package com.springkafka.kafka_app.utils;

import java.util.ArrayList;
import java.util.Collection;

public enum TopicEnum {
    TOPIC("topic"),
    TOPIC1("topic_add_to_cart"),
    TOPIC2("topic_add_to_wishlist"),
    TOPIC3("topic_buy_now");

    private final String topicName;

    TopicEnum(String topicName) {
        this.topicName = topicName;
    }

    public String getTopicName() {
        return topicName;
    }

    public static Collection<String> getAllTopicNames() {
        Collection<String> topics = new ArrayList<>();

        for (TopicEnum topic : values()) {
            topics.add(topic.getTopicName());
        }

        return topics;
    }
}

