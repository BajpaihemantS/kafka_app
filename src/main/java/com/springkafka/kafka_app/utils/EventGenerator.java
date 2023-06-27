package com.springkafka.kafka_app.utils;

import com.springkafka.kafka_app.event.Event;
import com.springkafka.kafka_app.wrapper.ExecutorServiceWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class EventGenerator {

    private final Random random = new Random();

    public List<Event> generateNEvents(int n){
        List<Event> eventList = new ArrayList<>();
        for (int i=0;i<n;i++){
            eventList.add(generateEvent());
        }
        return eventList;
    }

    public Event generateEvent(){
        Event event = new Event();
        int field = random.nextInt(5);
        for(int i=0;i<field;i++) {
            String key = generateKey();
            Object value = generateValue();
            event.setMapKeyValue(key, value);
        }
        int check = random.nextInt(2);
        event.setMapKeyValue("name","Hemant");
        event.setMapKeyValue("eventType","add_to_Wishlist");
        if(check==1){
            event.setMapKeyValue("productId","1");
        }
        else {
            event.setMapKeyValue("productId","2");
        }
        return event;
    }

    public String generateKey(){
        int length = random.nextInt(5) + 1;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            char randomChar = (char) (random.nextInt(26) + 'a');
            sb.append(randomChar);
        }
        return sb.toString();
    }

    public Object generateValue(){
        int valueType = random.nextInt(3);
        switch (valueType) {
            case 0:
                return generateKey();
            case 1:
                return random.nextInt(100);
            case 2:
                return random.nextBoolean();
            default:
                return null;
        }
    }
}
