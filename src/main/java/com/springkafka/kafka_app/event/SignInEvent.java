package com.springkafka.kafka_app.event;

import org.apache.kafka.common.protocol.types.Field;

public class SignInEvent extends Event{
    String userId;
    String password;
    private long timestamp;

    public SignInEvent() {
        this.timestamp = System.currentTimeMillis();
    }

    public SignInEvent(String userId, String password) {
        this.userId = userId;
        this.password = password;
        this.timestamp = System.currentTimeMillis();
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

}
