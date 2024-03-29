package com.springkafka.kafka_app.utils;

import com.springkafka.kafka_app.event.Event;
import com.springkafka.kafka_app.utils.Query.AgeRange;
import com.springkafka.kafka_app.utils.Query.User;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * This class generates random events which helps me in testing my code.
 * This generates random values for the incoming event.
 */


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
        String key = "name";
        String value = generateRandomString(5);
        event.setMapKeyValue(key,value);
        User user = new User("kanpur", new AgeRange(0,100));
        event.setMapKeyValue("userProperties",user);
        int check = random.nextInt(2);
        event.setMapKeyValue("timestamp",System.currentTimeMillis());
//        if(check==1){
//            event.setMapKeyValue("eventType","add_to_wishlist");
//            event.setMapKeyValue("productId","1");
//        }
//        else if(check==2){
//            event.setMapKeyValue("eventType","add_to_cart");
//            event.setMapKeyValue("productId","2");
//        }
        if(check==1){
            event.setMapKeyValue("eventType","buy_now");
        }
//        else if(check==4){
//            event.setMapKeyValue("eventType","sign_in");
//            event.setMapKeyValue("productId","4");
//        }
        else{
            event.setMapKeyValue("eventType","sign_out");
        }
        check = random.nextInt(2);
        if(check==1){
            event.setMapKeyValue("productId","3");
        }
        else{
            event.setMapKeyValue("productId","5");
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

    private static String generateRandomString(int length) {
        String characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuilder sb = new StringBuilder(length);
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(characters.length());
            char randomChar = characters.charAt(index);
            sb.append(randomChar);
        }
        return sb.toString();
    }

    public Object generateValue(){
        int valueType = random.nextInt(3);
        return switch (valueType) {
            case 0 -> generateKey();
            case 1 -> random.nextInt(100);
            case 2 -> random.nextBoolean();
            default -> null;
        };
    }
}
