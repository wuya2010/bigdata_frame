package com.alibaba.wordcount.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	IntWritable v = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
                          Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

		//先对Map端输出的数据进行局部汇总。
		
		int sum = 0 ;
		
		for (IntWritable value : values) {
			sum += value.get();
		}
		v.set(sum);
		//写出
		context.write(key, v);
	}
}
