package com.springkafka.kafka_app.utils;

import org.apache.kafka.common.protocol.types.Field;

public class LatencyCalculator {

    private static Long MIN_LATENCY = Long.MAX_VALUE;
    private static Long MAX_LATENCY = Long.MIN_VALUE;
    private static Long TOTAL_LATENCY = 0L;
    private static Long TOTAL_RECORDS = 0L;

    public static void checkMinLatency(Long latency){
        MIN_LATENCY = Long.min(latency,MIN_LATENCY);
    }

    public static void checkMaxLatency(Long latency){
        MAX_LATENCY = Long.max(latency,MAX_LATENCY);
    }

    public static void checkAndAddLatency(Long latency){
        synchronized (LatencyCalculator.class) {
            TOTAL_LATENCY += latency;
            TOTAL_RECORDS++;
            checkMaxLatency(latency);
            checkMinLatency(latency);
        }
    }


    public static String printStats(){
        if(TOTAL_RECORDS==0L) {
            return "No records found";
        }
        String minLatency = "Minimum latency is " + MIN_LATENCY + " ms";
        String maxLatency = "Maximum latency is " + MAX_LATENCY + " ms";
        String averageLatency = "Average latency is " + (TOTAL_LATENCY / TOTAL_RECORDS) + " ms";
        return minLatency + "\n" + maxLatency +"\n" + averageLatency;
    }

}
