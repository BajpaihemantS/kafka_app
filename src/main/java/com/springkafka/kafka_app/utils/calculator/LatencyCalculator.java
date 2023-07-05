package com.springkafka.kafka_app.utils.calculator;

import org.apache.kafka.common.protocol.types.Field;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class LatencyCalculator {

    private static long MIN_LATENCY = Long.MAX_VALUE;
    private static long MAX_LATENCY = Long.MIN_VALUE;
    private static final AtomicLong TOTAL_LATENCY = new AtomicLong(0);
    private static final AtomicLong TOTAL_RECORDS = new AtomicLong(0);
    private static Map<Long, AtomicInteger> frequencyMap = new ConcurrentHashMap<>();
    private static long modLatency = 0;
    private static long highestLatencyCount = 0;


    public static void checkMinLatency(Long latency){

        MIN_LATENCY = Long.min(latency,MIN_LATENCY);

    }

    public static void checkMaxLatency(Long latency){

        MAX_LATENCY = Long.max(latency,MAX_LATENCY);

    }

    public static synchronized void addModLatency(long latency){
        frequencyMap.computeIfAbsent(latency, key -> new AtomicInteger()).incrementAndGet();
    }

    public static void setModLatency(){
        Long modeLatency = null;
        int maxFrequency = 0;

        for (Map.Entry<Long, AtomicInteger> entry : frequencyMap.entrySet()) {
            long latency = entry.getKey();
            int frequency = entry.getValue().get();

            if (frequency > maxFrequency) {
                maxFrequency = frequency;
                modeLatency = latency;
            }
        }

        modLatency = modeLatency;
        highestLatencyCount = maxFrequency;
    }

    public static void checkAndAddLatency(Long latency){
            TOTAL_LATENCY.addAndGet(latency);
            TOTAL_RECORDS.incrementAndGet();
            checkMaxLatency(latency);
            checkMinLatency(latency);
//            addModLatency(latency);
    }

    public static String printStats(){
        if(TOTAL_RECORDS.longValue()==0L) {
            return "No records found";
        }

//        setModLatency();

        String minLatency = "Minimum latency is " + MIN_LATENCY + " ms\n";
        String maxLatency = "Maximum latency is " + MAX_LATENCY + " ms\n";
        String averageLatency = "Average latency is " + (TOTAL_LATENCY.longValue() / TOTAL_RECORDS.longValue()) + " ms\n";
        String mostLatency = "The latency " + modLatency + " appeared the most " + highestLatencyCount + " times\n";
        String records = "This data corresponds to " + TOTAL_RECORDS + " records";
        return minLatency + maxLatency + averageLatency + mostLatency + records;
    }

}