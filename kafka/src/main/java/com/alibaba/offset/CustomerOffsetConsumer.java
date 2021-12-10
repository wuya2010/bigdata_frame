package com.alibaba.offset;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Properties;

//方法： 自定义offset
public class CustomerOffsetConsumer {

	//创建一个集合，放topicPartition的信息  ： 根据需要定义变量
	//避免一次提交一次： currentOffsets.put()
	private static HashMap<TopicPartition,Long> currentOffsets = new HashMap<TopicPartition,Long>();

	public static void main(String[] args) {

		 	Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node03:9092,node04:9092,node05:9092");
	        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test07");


	        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


	        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);




	        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);



	        //可以传入的参数 ，consumerRebalanceListener  , 对分区重新分配  ： 匿名内部类
	        consumer.subscribe(Arrays.asList("media_test2"), new ConsumerRebalanceListener() {


	        	//第一步：回收所有分区，分配之前调用
				@Override
				public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

				}



				//第二步： 分配完成之后调用==》可以获取最新的offsest  ===> 这个地方获取offset

				// 进行原子性，在关系型数据库中
				@Override
				public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

					//++++++++++++++每次分配之后，都把这个清空，不清空分区信息会保持原来的不变
					currentOffsets.clear();

					//遍历所有分区==》这个表达要知道具体的含义
					for (TopicPartition partition : partitions) {

						//返回一个long类型的值，，获取offset
						Long offset = getOffsetByTopicPartition(partition);
						//获取位置：定位当前的offset
						consumer.seek(partition, offset);
					}


				}



				private Long getOffsetByTopicPartition(TopicPartition partition) {
					// TODO Auto-generated method stub
					return null;
				}
			});




	        //提交offset的逻辑
			while(true) {

				ConsumerRecords<String, String> records = consumer.poll(100);


				//+++++++++++精确一次消费： 放到同一个事务中

				//tx.begin;s

				for (ConsumerRecord<String, String> record : records) {

					System.out.println(record.value());

					//record可以获取topicpatitions， 创建一个map集合,利用了map的特点，会将相同key的map进行覆盖
					//topic, partition, offset
					currentOffsets.put(new TopicPartition(record.topic(), record.partition()),record.offset());

				}

				commitOffset(currentOffsets);

				//tx.commit;
			}


			//循环条件下，怎么关闭consumer
	}

	private static void commitOffset(HashMap<TopicPartition, Long> currentOffsets2) {
		// TODO Auto-generated method stub

	}


}
