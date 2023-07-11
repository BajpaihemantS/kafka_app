package com.springkafka.kafka_app.service.scheduler;

import com.springkafka.kafka_app.service.kafka_consumer.ConsumerKafka;
import com.springkafka.kafka_app.utils.Query.Query;
import com.springkafka.kafka_app.utils.calculator.QueryCheckAndPrint;
import com.springkafka.kafka_app.wrapper.CustomLogger;
import com.springkafka.kafka_app.wrapper.ExecutorServiceWrapper;

import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * this is the scheduler which prints the current users which satisfy our query
 * First, this calls for a consumer and then in interval of 5 seconds prints the users
 */

public class Scheduler extends CustomLogger {

    private final ConsumerKafka kafka_consumer;
    private final Query query;
    private final String outputTopic;

    public Scheduler(ConsumerKafka kafka_consumer, Query query, String outputTopic) {
        this.kafka_consumer = kafka_consumer;
        this.query = query;
        this.outputTopic = outputTopic;
    }

    public void startScheduling(AtomicInteger queryCount) {
        int newQueryCount = queryCount.intValue();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        HashSet<String> userSet = new HashSet<>();
        scheduler.submit(() -> kafka_consumer.consumeEvents(outputTopic,query,userSet));
        scheduler.scheduleAtFixedRate(() -> startStreams(userSet,newQueryCount), 0, 10, TimeUnit.SECONDS);
    }

    private void startStreams(HashSet<String> userSet, int newQueryCount) {
        info("Users satisfying the query-{} ", newQueryCount);
        QueryCheckAndPrint.printUsers(userSet);
    }
}
