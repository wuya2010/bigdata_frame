package com.alibaba.offset;


//自定义offset

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

public class CustomerOffsetConsumer2 {

	public static void main(String[] args) {
		
		
	    Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node03:9092,node04:9092,node05:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test08");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		
		
        //测试环节
        // 接口：  ConsumerRebalanceListener   需要实现2个方法
		consumer.subscribe(Arrays.asList("media_test2"), new ConsumerRebalanceListener() {
			
			
			//回收所有分区   消费者所有分区的集合： Collection
			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				
				System.out.println("before reBalance");
				
				for (TopicPartition partition : partitions) {
					
					System.out.println("partition" + partition);
				}
				
				
			}
			
			
			
			
			
			//调用重新分区之后调用
			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

				System.out.println("after reBalance");
				
			     for (TopicPartition partition : partitions) {
	                    System.out.println("partition = " + partition);
	                }
				
			}
		});
		
	    try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.value());
                }
            }
        } finally {
            consumer.close();
        }
		
		
	}
	
}
