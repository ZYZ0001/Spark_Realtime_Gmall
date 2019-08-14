package com.atguigu.gmall.canal.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

/**
 * Kafka发送消息类
 */
public class MyKafkaSender {

    public static KafkaProducer<String, String> kafkaProducer = null;

    // 创建KafkaProducer对象
    public static KafkaProducer<String, String> createKafkaProducer() {
        Properties pro = new Properties();
        try {
            pro.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("kafka.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        KafkaProducer<String, String> producer = new KafkaProducer<>(pro);
        return producer;
    }

    // 向Kafka发送数据
    public static void send(String topic, String msg) {
        if (kafkaProducer == null) {
            kafkaProducer = createKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord<String, String>(topic, msg));
    }
}
