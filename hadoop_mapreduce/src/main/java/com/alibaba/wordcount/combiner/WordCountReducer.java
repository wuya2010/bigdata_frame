package com.alibaba.wordcount.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reduce阶段
 * 
 * 通过继承 Reducer,重写 reduce方法,完成自定义的Reducer
 * 
 *
 */
public class WordCountReducer  extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	IntWritable v = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
                          Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		//1.将相同key的value进行汇总.
			//  atguigu 1 
			//  atguigu 1 
		int sum = 0 ; 
		for (IntWritable value : values) {
			sum+=value.get();
		}
		
		v.set(sum);
		//2. 写出
		context.write(key, v );
	}
}
