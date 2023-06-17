package com.springkafka.kafka_app.democontroller;

import com.springkafka.kafka_app.service.Producer;
import com.springkafka.kafka_app.utils.TopicEnum;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/producer")
public class KafkaRestController {

    Producer producer;
    private String message;

    @Autowired
    public KafkaRestController(Producer producer) {
        this.producer = producer;
    }

//    @GetMapping("/add-to-cart")
//    public void getMessageForAddToCart(@RequestParam("message") String message){
//        producer.sendMessage(TopicEnum.TOPIC1.getTopicName(),message);
//    }

    @GetMapping("/add-to-cart")
    public void getMessageForAddToCart(@RequestBody String message){
        producer.sendMessage(TopicEnum.TOPIC1.getTopicName(),message);
    }

    @GetMapping("/buy-now")
    public void getMessageForBuyNow(@RequestBody String message){
        producer.sendMessage(TopicEnum.TOPIC3.getTopicName(),message);
    }

    @GetMapping("/add-to-wishlist")
    public void getMessageForWishList(@RequestBody String message){
        producer.sendMessage(TopicEnum.TOPIC2.getTopicName(),message);
    }

}
