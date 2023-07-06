package com.springkafka.kafka_app.config;

import com.springkafka.kafka_app.wrapper.CustomLogger;
import com.springkafka.kafka_app.utils.TopicEnum;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 *
 * Class to get the topics deleted once the process has ended
 *
 */

@Component
public class KafkaTopicManager extends CustomLogger implements SmartLifecycle {

//  Creating an instance of AdminClient and then assigning it with the required properties
    private final AdminClient adminClient;

    public KafkaTopicManager() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        this.adminClient = AdminClient.create(properties);
//        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    @Override
    public void start() {

    }

    public void createTopics(List<String> topics)  {
        List<NewTopic> newTopicCollection = new ArrayList<>();
        for(String topic : topics){
            NewTopic newTopic = new NewTopic(topic,1, (short) 1);
            newTopicCollection.add(newTopic);
        }
        adminClient.createTopics(newTopicCollection);
        try{
            Thread.sleep(5000);
        }
        catch (Exception e){
            error("failed with error",e);
        }
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

    @Override
    public boolean isRunning() {
        return true;
    }

}
