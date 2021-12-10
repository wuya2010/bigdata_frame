package com.alibaba.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CallbackProducer {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node03:9092,node04:9092,node05:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 1);

        //1.创建KafkaProducer对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        //2.调用send方法, 很快就能执行完成
        //   这个回调通常会在后台I/O线程中执行，因此速度应该很快。
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("media_test2", "message-" + i);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        // success:media_test2-0-271 数据写入
                        System.out.println("success:" + metadata.topic() + "-" + metadata.partition() + "-" + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });
        }

        producer.close();
    }
}
