package com.lrc.example;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * Created by home1 on 2016/11/2.
 */
public class KafkaUtil {
    private static KafkaProducer<String, String> kp;
    private static KafkaConsumer<String, String> kc;

    public static KafkaProducer<String, String> getProducer() {
        if (kp == null) {
            Properties props = new Properties();
            props.put("bootstrap.servers", "192.168.1.232:9092,192.168.1.233:9092,192.168.1.234:9092");
            props.put("acks", "1");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kp = new KafkaProducer<String, String>(props);
        }
        return kp;
    }

    public static KafkaConsumer<String, String> getConsumer() {
        if (kc == null) {
            Properties props = new Properties();
            props.put("bootstrap.servers", "192.168.1.232:9092,192.168.1.233:9092,192.168.1.234:9092");
            props.put("group.id", "test_group1");
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("session.timeout.ms", "30000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            kc = new KafkaConsumer<String, String>(props);
        }
        return kc;
    }
}
