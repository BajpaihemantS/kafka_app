package com.springkafka.kafka_app.democontroller;

import com.springkafka.kafka_app.service.ConsumerRunner;
import com.springkafka.kafka_app.service.ProducerRunner;
import com.springkafka.kafka_app.utils.TopicEnum;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/producer")
public class KafkaRestController {

    @GetMapping("/add-to-cart")
    public void getMessageForAddToCart(@RequestBody String message) throws Exception{
        try {
            ProducerRunner.runProducer(TopicEnum.TOPIC1.getTopicName(),message);
        } catch (Exception e) {
            System.out.println("Exception returned " + e);
            e.printStackTrace();
        }
    }

    @GetMapping("/buy-now")
    public void getMessageForBuyNow(@RequestBody String message){
        try {
            ProducerRunner.runProducer(TopicEnum.TOPIC3.getTopicName(),message);
        } catch (Exception e) {
            System.out.println("Exception returned " + e);
            e.printStackTrace();
        }
    }

    @GetMapping("/add-to-wishlist")
    public void getMessageForWishList(@RequestBody String message) {
        try {
            ProducerRunner.runProducer(TopicEnum.TOPIC2.getTopicName(), message);
        } catch (Exception e) {
            System.out.println("Exception returned " + e);
            e.printStackTrace();
        }
    }

    @GetMapping("/read-messages")
    public void readProducedMessages(){
        ConsumerRunner.runConsumer();
    }

}
