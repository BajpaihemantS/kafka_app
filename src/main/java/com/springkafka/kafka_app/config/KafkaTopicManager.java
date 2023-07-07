package com.springkafka.kafka_app.config;

import com.springkafka.kafka_app.wrapper.CustomLogger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 *
 * Class to get the topics deleted once the process has ended
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
        try{
            adminClient.deleteTopics(topicList);
        }catch (Exception e){
            error("Error deleting the topics",e);
        }
    }

    public void createTopics(List<String> topics)  {
        List<NewTopic> newTopicCollection = new ArrayList<>();
        for(String topic : topics){
            NewTopic newTopic = new NewTopic(topic,1, (short) 1);
            newTopic.configs(
                    Map.of(
                            "cleanup.policy", "compact",
                            "min.cleanable.dirty.ratio", "0.001",
                            "segment.ms", "100",
                            "max.compaction.lag.ms","10",
                            "min.compaction.lag.ms","1",
                            "log.cleanup.interval.ms","5000"
                    )
            );
            newTopicCollection.add(newTopic);
        }
        adminClient.createTopics(newTopicCollection);
        try{
            info("the following topics were created {}",topics);
            Thread.sleep(5000);
        }
        catch (Exception e){
            error("failed with error",e);
        }
    }

    @Override
    public boolean isRunning() {
        return true;
    }

}