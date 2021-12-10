
package com.alibaba.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Properties;

public class CustomOffsetConsumer {

    private static HashMap<TopicPartition, Long> currentOffsets = new HashMap<>();

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node03:9092,node04:9092,node05:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test06");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList("media_test2"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                currentOffsets.clear();
                for (TopicPartition partition : partitions) {
                    Long offset = getOffsetByTopicPartition(partition);
                    System.out.println("get offset "  + offset);
                    consumer.seek(partition, offset);
                }
            }
        });

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);

                //tx.begin
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.value());
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), record.offset());
                }
                commitOffset(currentOffsets);
                //tx.commit
            }
        } finally {
            consumer.close();
        }
    }

    private static void commitOffset(HashMap<TopicPartition, Long> currentOffsets) {
        System.out.println(currentOffsets);

        /**
         * {media_test2-0=270, media_test2-1=241, media_test2-2=308}
         * {media_test2-0=270, media_test2-1=241, media_test2-2=308}
         * {media_test2-0=270, media_test2-1=241, media_test2-2=308}
         * {media_test2-0=270, media_test2-1=241, media_test2-2=308}
         * {media_test2-0=270, media_test2-1=241, media_test2-2=308}
         */
    }

    private static Long getOffsetByTopicPartition(TopicPartition partition) {
        return null;
    }
}

