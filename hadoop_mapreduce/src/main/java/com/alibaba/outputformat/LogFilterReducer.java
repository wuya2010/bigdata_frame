package com.alibaba.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LogFilterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<NullWritable> values,
                          Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		
		System.out.println("Key:" + key );
		
		context.write(key, NullWritable.get());
	}
}
