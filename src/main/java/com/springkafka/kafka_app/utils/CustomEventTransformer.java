package com.springkafka.kafka_app.utils;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class CustomEventTransformer implements Transformer<String, String, KeyValue<String, String>> {

    private ProcessorContext context;
    private KeyValueStore<String, Long> stateStore;


    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.stateStore = context.getStateStore(ServiceProperties.USER_COUNT_STORE);
    }

    @Override
    public KeyValue<String, String> transform(String key, String value) {

        String user = key;
        Long userCount = stateStore.get(user);

//        eventList = stateStore.get(queryUserEventProduct);
//        if(eventList==null){
//            eventList = new EventList();
//        }
//        eventList.add(value);
//        stateStore.put(queryUserEventProduct,eventList);

        return KeyValue.pair(user,user);
    }

    @Override
    public void close() {
    }

}
