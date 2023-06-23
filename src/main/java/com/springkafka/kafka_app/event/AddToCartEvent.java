package com.springkafka.kafka_app.event;

import org.apache.kafka.common.protocol.types.Field;

import java.time.Instant;

public class AddToCartEvent {
    private String productId;
    private String productName;
    private int quantity;
    private double price;
    private long timestamp;

    public AddToCartEvent() {
        this.timestamp = System.currentTimeMillis();
    }

    public AddToCartEvent(String productId, String productName, int quantity, double price) {
        this.productId = productId;
        this.productName = productName;
        this.quantity = quantity;
        this.price = price;
        this.timestamp = System.currentTimeMillis();
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}
