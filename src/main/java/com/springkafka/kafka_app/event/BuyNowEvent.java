package com.springkafka.kafka_app.event;

public class BuyNowEvent extends Event{
    private String productId;
    private String productName;
    private int quantity;
    private double price;
    private String paymentMethod;
    private long timestamp;

    public BuyNowEvent() {
        this.timestamp = System.currentTimeMillis();
    }

    public BuyNowEvent(String productId, String productName, int quantity, double price, String paymentMethod, long timestamp) {
        this.productId = productId;
        this.productName = productName;
        this.quantity = quantity;
        this.price = price;
        this.paymentMethod = paymentMethod;
        this.timestamp = timestamp;
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

    public String getPaymentMethod() {
        return paymentMethod;
    }

    public void setPaymentMethod(String paymentMethod) {
        this.paymentMethod = paymentMethod;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
