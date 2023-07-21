package com.springkafka.kafka_app.utils.calculator;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This function measures the latency
 * and finds various metrics related to it.
 */

public class LatencyCalculator {

    private static long MIN_LATENCY = Long.MAX_VALUE;
    private static long MAX_LATENCY = Long.MIN_VALUE;
    private static final AtomicLong TOTAL_LATENCY = new AtomicLong(0);
    private static final AtomicLong TOTAL_RECORDS = new AtomicLong(0);
    private final static Map<Long, AtomicInteger> frequencyMap = new ConcurrentHashMap<>();

    public static void checkMinLatency(Long latency) {

        MIN_LATENCY = Long.min(latency, MIN_LATENCY);

    }

    public static void checkMaxLatency(Long latency) {

        MAX_LATENCY = Long.max(latency, MAX_LATENCY);

    }

    public static synchronized void addModLatency(long latency) {
        frequencyMap.computeIfAbsent(latency, key -> new AtomicInteger()).incrementAndGet();
    }
    public static void checkAndAddLatency(Long latency) {
        TOTAL_LATENCY.addAndGet(latency);
        TOTAL_RECORDS.incrementAndGet();
        checkMaxLatency(latency);
        checkMinLatency(latency);
        addModLatency(latency);
    }

    public static long calculateP99Latency() {
        long[] latencies = frequencyMap.keySet().stream().mapToLong(Long::longValue).toArray();
        Arrays.sort(latencies);
        int p99Index = (int) Math.ceil(latencies.length * 0.99);
        if (p99Index == latencies.length) p99Index--; // this is to check if the p99 index does not exceed the array length
        return latencies[p99Index];
    }

    public static String printStats() {
        if (TOTAL_RECORDS.longValue() == 0L) {
            return "No records found";
        }

        String minLatency = "Minimum latency is " + MIN_LATENCY + " ms\n";
        String maxLatency = "Maximum latency is " + MAX_LATENCY + " ms\n";
        String averageLatency = "Average latency is " + (TOTAL_LATENCY.longValue() / TOTAL_RECORDS.longValue()) + " ms\n";
        String p99Latency = "p99 latency is " + calculateP99Latency() + " ms\n";
        String records = "This data corresponds to " + TOTAL_RECORDS + " records";
        return minLatency + maxLatency + averageLatency + p99Latency + records;
    }

}
