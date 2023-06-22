package com.springkafka.kafka_app.utils;

import org.apache.kafka.common.protocol.types.Field;

import java.util.concurrent.atomic.AtomicLong;

public class LatencyCalculator {

    private static Long MIN_LATENCY = Long.MAX_VALUE;
    private static Long MAX_LATENCY = Long.MIN_VALUE;
    private static final AtomicLong TOTAL_LATENCY = new AtomicLong(0);
    private static final AtomicLong TOTAL_RECORDS = new AtomicLong(0);

    public static void checkMinLatency(Long latency){
        MIN_LATENCY = Long.min(latency,MIN_LATENCY);
    }

    public static void checkMaxLatency(Long latency){
        MAX_LATENCY = Long.max(latency,MAX_LATENCY);
    }

    public static void checkAndAddLatency(Long latency){
            TOTAL_LATENCY.addAndGet(latency);
            TOTAL_RECORDS.incrementAndGet();
            checkMaxLatency(latency);
            checkMinLatency(latency);
    }

    public static String printStats(){
        if(TOTAL_RECORDS.longValue()==0L) {
            return "No records found";
        }
        String minLatency = "Minimum latency is " + MIN_LATENCY + " ms\n";
        String maxLatency = "Maximum latency is " + MAX_LATENCY + " ms\n";
        String averageLatency = "Average latency is " + (TOTAL_LATENCY.longValue() / TOTAL_RECORDS.longValue()) + " ms\n";
        String records = "This data corresponds to " + TOTAL_RECORDS + " records";
        return minLatency + maxLatency + averageLatency + records;
    }

}
