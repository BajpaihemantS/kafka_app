package com.springkafka.kafka_app.config;

import com.springkafka.kafka_app.wrapper.CustomLogger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Properties;

/**
 *
 * Class to get the topics deleted once the process has ended.
 * AdminClient manages the topics in the kafka broker
 *
 */

@Component
public class KafkaTopicManager extends CustomLogger implements SmartLifecycle {

    private final AdminClient adminClient;

    public KafkaTopicManager() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        this.adminClient = AdminClient.create(properties);
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
    }

    public void deleteTopics(List<String> topicList){
        adminClient.deleteTopics(topicList);
        info("The topics have been deleted");
    }

    @Override
    public boolean isRunning() {
        return true;
    }

}