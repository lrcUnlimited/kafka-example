package com.lrc.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Created by home1 on 2016/11/3.
 */
public class KafkaProducerTest {
    public static void main(String[] args) {
        Producer<String, String> producer = KafkaUtil.getProducer();
        int i = 0;
        while (true) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("my-replicated-test",
                    Integer.toString(i), "this is message" + i);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null)
                        e.printStackTrace();
                    System.out.println("message send to partition " + metadata.partition() +
                            ", offset: " + metadata.offset());
                }
            });
            i++;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
