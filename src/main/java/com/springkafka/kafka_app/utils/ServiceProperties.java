package com.springkafka.kafka_app.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

public interface ServiceProperties  {
    String KAFKA_BROKERS = "localhost:9092";
    String CLIENT_ID="client2";
    String OFFSET_RESET_LATEST="latest";
    String OFFSET_RESET_EARLIER="earliest";
    Integer MAX_POLL_RECORDS=1;
    Integer MAX_PRODUCER = 100;
    Integer MAX_CONSUMER = 1;
    Integer MAX_EVENTS = 1;
    Integer MAX_NO_MESSAGE_FOUND_COUNT=4;
    ObjectMapper objectmapper = new ObjectMapper();
    String ATTRIBUTE_COUNT_STORE = "attributeValueStore";
    String USER_COUNT_STORE = "userCountStore";

}

