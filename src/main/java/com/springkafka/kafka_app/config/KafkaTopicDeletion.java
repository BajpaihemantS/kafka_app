package com.springkafka.kafka_app.config;

import com.springkafka.kafka_app.wrapper.CustomLogger;
import com.springkafka.kafka_app.utils.TopicEnum;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.Properties;

/**
 *
 * Class to get the topics deleted once the process has ended
 *
 */

@Component
public class KafkaTopicDeletion extends CustomLogger implements SmartLifecycle {

//  Creating an instance of AdminClient and then assigning it with the required properties
    private final AdminClient adminClient;

    public KafkaTopicDeletion() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        this.adminClient = AdminClient.create(properties);
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    @Override
    public void start() {

    }

// Deleting the topics
    @Override
    public void stop() {
        try {
            adminClient.deleteTopics(TopicEnum.getAllTopicNames());
            adminClient.close();
            info("Topics deleted successfully");

        } catch (Exception e) {
            error("Error deleting topics: ", e);
        }
    }

    @Override
    public boolean isRunning() {
        return true;
    }

}
