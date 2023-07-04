package com.springkafka.kafka_app.event;

import java.util.ArrayList;
import java.util.List;

public class EventList {

    private List<Event> eventList;

    public EventList() {
        eventList = new ArrayList<>();
    }

    public void add(Event name){
        eventList.add(name);
    }

    public List<Event> getList(){
        return eventList;
    }
}
