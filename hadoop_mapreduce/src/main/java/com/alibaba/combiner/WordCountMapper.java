package com.alibaba.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Map阶段
 * 
 * 需要继承Mapper,并重写map方法， 完成自定义的Mapper ， 
 * 
 * KEYIN : 输入的Key的类型
 * VALUEIN: 输入的Value的类型
 * 
 * KEYOUT: 输出的Key的类型
 * VALUEOUT: 输出的Value的类型
 * 
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	Text k = new Text();
	IntWritable v = new IntWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//1. 获取一行数据  ,例如:   atguigu atguigu
		String line  = value.toString();
		
		//2. 切割数据 [atguigu,atguigu]
		String[] split = line.split(" ");  
		
		//3. 写出数据
		for (String word : split) {
			k.set(word);
			context.write(k,v);
		}
	}
	
	
	
	
	
	
}
