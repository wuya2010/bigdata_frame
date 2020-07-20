package com.alibaba.kv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class KvTextInputFormatDriver {
	
	public static void main(String[] args) throws Exception {
		
		args = new String[] {"d:/input/inputkv","d:/output"};
		
		//1. 获取Job
		Configuration conf = new Configuration();
		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
		Job job = Job.getInstance(conf);
	
		//2. 关联jar
		job.setJarByClass(KvTextInputFormatDriver.class);
		
		//3. 关联mapper 和 Reducer
		job.setMapperClass(KvTextInputFormatMapper.class);
		job.setReducerClass(KvTextInputFormatReducer.class);
		//4. 设置Mapper输出的key和value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//5. 设置最终输出的key和value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//6. 设置输入和输出路径 
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//设置keyValueTextInputFormat
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		//7. 提交Job
		job.waitForCompletion(true);
	}
}
