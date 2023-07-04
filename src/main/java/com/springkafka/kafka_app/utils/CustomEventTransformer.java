package com.springkafka.kafka_app.utils;

import com.springkafka.kafka_app.event.Event;
import com.springkafka.kafka_app.event.EventList;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;

public class CustomEventTransformer implements Transformer<String, Event, KeyValue<String, Event>> {

    private ProcessorContext context;
    private KeyValueStore<String, EventList> stateStore;


    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.stateStore = context.getStateStore(ServiceProperties.STATE_STORE);
    }

    @Override
    public KeyValue<String, Event> transform(String key, Event value) {

        String eventType = value.getMapKeyValue("eventType").toString();
        EventList eventList = stateStore.get(eventType);
        if(eventList==null){
            eventList = new EventList();
        }
        eventList.add(value);
        stateStore.put(eventType,eventList);


        String productId = value.getMapKeyValue("productId").toString();
        eventList = stateStore.get(productId);
        if(eventList==null){
            eventList = new EventList();
        }
        eventList.add(value);
        stateStore.put(productId,eventList);


        String customerId = value.getMapKeyValue("name").toString();
        eventList = stateStore.get(customerId);
        if(eventList==null){
            eventList = new EventList();
        }
        eventList.add(value);
        stateStore.put(customerId,eventList);


        String queryUserEvent = customerId+eventType;
        eventList = stateStore.get(queryUserEvent);
        if(eventList==null){
            eventList = new EventList();
        }
        eventList.add(value);
        stateStore.put(queryUserEvent,eventList);


        String queryUserProduct = customerId+eventType;
        eventList = stateStore.get(queryUserProduct);
        if(eventList==null){
            eventList = new EventList();
        }
        eventList.add(value);
        stateStore.put(queryUserProduct,eventList);


        String queryEventProduct = customerId+eventType;
        eventList = stateStore.get(queryEventProduct);
        if(eventList==null){
            eventList = new EventList();
        }
        eventList.add(value);
        stateStore.put(queryEventProduct,eventList);


        String queryUserEventProduct = customerId+eventType+productId;
        eventList = stateStore.get(queryUserEventProduct);
        if(eventList==null){
            eventList = new EventList();
        }
        eventList.add(value);
        stateStore.put(queryUserEventProduct,eventList);

        return KeyValue.pair(key, value);
    }

    @Override
    public void close() {
    }

}
