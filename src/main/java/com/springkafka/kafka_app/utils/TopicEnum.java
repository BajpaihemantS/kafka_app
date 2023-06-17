package com.springkafka.kafka_app.utils;

public enum TopicEnum {
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

    public static String[] getAllTopicNames() {
       TopicEnum[] topics = TopicEnum.values();
        String[] topicNames = new String[topics.length];

        for (int i = 0; i < topics.length; i++) {
            topicNames[i] = topics[i].getTopicName();
        }

        return topicNames;
    }
}

