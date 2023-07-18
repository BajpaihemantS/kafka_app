package org.kafkaApp.utils.calculator;


import org.kafkaApp.event.Event;

import java.util.List;

public class TimestampCalculator {

    public static long getTimestampValue(List<Event> eventList, int val, char duration){
        long timestamp = 1L;
        if(duration=='S'){
            timestamp *= val * 1000L;
        }
        else if(duration=='H'){
            timestamp *= (long) val * 3600 * 1000;
        }
        else{
            timestamp *= (long) val * 30 * 24 * 3600 * 1000;
        }

        return timestamp;
    }
}
