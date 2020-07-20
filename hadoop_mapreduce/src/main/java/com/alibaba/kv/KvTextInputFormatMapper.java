package com.alibaba.kv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KvTextInputFormatMapper extends Mapper<Text, Text, Text, IntWritable> {
	
	IntWritable v = new IntWritable(1);
	
	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		System.out.println("key: " + key.toString() );
		System.out.println("value: " + value.toString());
		System.out.println("================================");
		context.write(key, v );
		
	}
}
