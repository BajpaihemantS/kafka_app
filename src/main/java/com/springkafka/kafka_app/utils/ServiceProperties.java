package com.springkafka.kafka_app.utils;

public interface ServiceProperties {
    public String KAFKA_BROKERS = "localhost:9092";

    public Integer MESSAGE_COUNT=1000;

    public String CLIENT_ID="client1";

    public String TOPIC_NAME="demo";

    public String GROUP_ID_CONFIG="consumerGroup1";

    public Integer MAX_NO_MESSAGE_FOUND_COUNT=10000;

    public String OFFSET_RESET_LATEST="latest";

    public String OFFSET_RESET_EARLIER="earliest";

    public Integer MAX_POLL_RECORDS=1;


}
