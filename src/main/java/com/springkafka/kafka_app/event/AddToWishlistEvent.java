package com.springkafka.kafka_app.event;

public class AddToWishlistEvent {
    private String userId;
    private String productName;
    private String productId;
    private long timestamp;

    public AddToWishlistEvent() {
        this.timestamp = System.currentTimeMillis();
    }

    public AddToWishlistEvent(String userId, String productName, String productId) {
        this.userId = userId;
        this.productName = productName;
        this.productId = productId;
        this.timestamp = System.currentTimeMillis();
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

}
