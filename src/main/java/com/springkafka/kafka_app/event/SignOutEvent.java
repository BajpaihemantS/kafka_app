package com.springkafka.kafka_app.event;

public class SignOutEvent {
    private String userId;
    private long sessionTime;
    private long timestamp;

    public SignOutEvent() {
        this.timestamp = System.currentTimeMillis();
    }

    public SignOutEvent(String userId, long sessionTime) {
        this.userId = userId;
        this.sessionTime = sessionTime;
        this.timestamp = System.currentTimeMillis();
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public long getSessionTime() {
        return sessionTime;
    }

    public void setSessionTime(long sessionTime) {
        this.sessionTime = sessionTime;
    }
}
