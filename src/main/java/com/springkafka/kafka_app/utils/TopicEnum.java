package com.springkafka.kafka_app.utils;

import java.util.ArrayList;
import java.util.Collection;

public enum TopicEnum {
    TOPIC("topic"),
    TOPIC1("topicadd_to_cart"),
    TOPIC2("topicadd_to_wishlist"),
    TOPIC3("topicbuy_now"),
    TOPIC4("topicsign_in"),
    TOPIC5("topicsign_out");

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

