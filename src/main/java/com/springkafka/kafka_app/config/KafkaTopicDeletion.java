package com.springkafka.kafka_app.config;

import com.springkafka.kafka_app.utils.TopicEnum;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class KafkaTopicDeletion implements SmartLifecycle {

    private final AdminClient adminClient;

    public KafkaTopicDeletion() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        this.adminClient = AdminClient.create(properties);
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
        try {
            adminClient.deleteTopics(TopicEnum.getAllTopicNames());
            System.out.println("Topics deleted successfully.");
        } catch (Exception e) {
            System.err.println("Error deleting topics: " + e.getMessage());
        }
    }

    @Override
    public boolean isRunning() {
        return true;
    }

}
