package com.michael.tools.kafka.consumer;

import com.michael.tools.kafka.producer.KafkaProduceMsg;


public class DataConsumer {

    public static void main(String args[]) {

        // 1. init kafka consumer
        KafkaConsumeMsg kafkaMsg = new KafkaConsumeMsg();

        // 2. consume message from kafka
        kafkaMsg.consumeMsg();

    }
}
