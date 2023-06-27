package com.springkafka.kafka_app.utils;

import com.springkafka.kafka_app.event.Event;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class CustomTransformer implements Transformer<String, Event, KeyValue<String, Event>> {

    private ProcessorContext context;
    private KeyValueStore<String, String> stateStore;


    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.stateStore = (KeyValueStore<String, String>) context.getStateStore(ServiceProperties.WISHLIST_STORE);
    }

    @Override
    public KeyValue<String, Event> transform(String key, Event value) {
        String name = value.getMapKeyValue("name").toString();
        String id = value.getMapKeyValue("productId").toString();

        stateStore.put(id,name);
        System.out.println(id + " ---- --- --- - - - - ---------"  + stateStore.get(id));

        return KeyValue.pair(key, value);
    }

    @Override
    public void close() {
        // Clean up resources if needed
    }

}
