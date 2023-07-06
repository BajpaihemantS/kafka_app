package com.springkafka.kafka_app.utils.calculator;

import com.springkafka.kafka_app.event.Event;

import java.util.List;

public class TimestampCalculator {

    public static int findIndex(List<Event> eventList, long timestamp){

        int low = 0, high = eventList.size();
        int mid;
        while (low < high) {

            mid = low + (high - low) / 2;
            if (timestamp <= Long.parseLong(eventList.get(mid).getMapKeyValue("timestamp").toString())) {
                high = mid;
            }
            else {
                low = mid + 1;
            }
        }
        if (low < eventList.size() && Long.parseLong(eventList.get(low).getMapKeyValue("timestamp").toString()) < timestamp) {
            low++;
        }
        return low;
    }

    public static int getTimestampValue(List<Event> eventList, int val, char duration){
        long timestamp = 1L;
        if(duration=='S'){
            timestamp *= val * 1000L;
            return findIndex(eventList,timestamp);
        }
        else if(duration=='H'){
            timestamp *= (long) val * 3600 * 1000;
            return findIndex(eventList,timestamp);
        }
        else{
            timestamp *= (long) val * 30 * 24 * 3600 * 1000;
            return findIndex(eventList,timestamp);
        }

    }
}
