package org.kafkaApp.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This was the class which contains the configuration properties
 * and the values of the constants
 */

public interface ServiceProperties  {
    String KAFKA_BROKERS = "localhost:9092";
    String CLIENT_ID = "clientId";
    String USER_PROPERTIES = "userProperties";
    String AGE = "age";
    String LOCATION = "location";
    String NAME = "name";
    String TIMESTAMP = "timestamp";
    String OFFSET_RESET_LATEST="latest";
    String OFFSET_RESET_EARLIER="earliest";
    Integer MAX_POLL_RECORDS=1;
    Integer MAX_PRODUCER = 1000;
    Integer MAX_CONSUMER = 1;
    Integer MAX_EVENTS = 100;
    Integer MAX_NO_MESSAGE_FOUND_COUNT=30;
    ObjectMapper objectmapper = new ObjectMapper();
    String ATTRIBUTE_COUNT_STORE = "attributeValueStore";

}

