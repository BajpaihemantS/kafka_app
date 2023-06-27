package com.springkafka.kafka_app.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ServiceProperties  {
    public String KAFKA_BROKERS = "localhost:9092";
    public String CLIENT_ID="client2";
    public String OFFSET_RESET_LATEST="latest";
    public String OFFSET_RESET_EARLIER="earliest";
    public Integer MAX_POLL_RECORDS=1;
    public Integer MAX_PRODUCER = 10;
    public Integer MAX_CONSUMER = 1;
    public Integer MAX_EVENTS = 1;
    public static Integer MAX_NO_MESSAGE_FOUND_COUNT=10;
    public static final ObjectMapper objectmapper = new ObjectMapper();
    public static final String WISHLIST_STORE = "wishlist_store";

}

