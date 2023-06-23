package com.springkafka.kafka_app.utils;

public interface ServiceProperties {
    public String KAFKA_BROKERS = "localhost:9092";

    public String CLIENT_ID="client2";


    public String OFFSET_RESET_LATEST="latest";

    public String OFFSET_RESET_EARLIER="earliest";

    public Integer MAX_POLL_RECORDS=1;

    public Integer MAX_PRODUCER = 100;

    public Integer MAX_CONSUMER = 1;

    public static Integer MAX_NO_MESSAGE_FOUND_COUNT=10;
}

