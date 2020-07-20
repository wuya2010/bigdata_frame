package com.alibaba.nline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NLineInputFormatReducer  extends Reducer<Text, IntWritable, Text, IntWritable> {
	IntWritable v = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
                          Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		
		//将key的所有元素集合在一起
		int sum = 0 ; 
		for (IntWritable value : values) {
			sum +=value.get();
		}
		v.set(sum);
		context.write(key, v);
	
	}
}
