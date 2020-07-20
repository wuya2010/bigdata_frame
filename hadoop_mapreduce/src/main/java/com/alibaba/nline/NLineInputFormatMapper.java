package com.alibaba.nline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NLineInputFormatMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Text k = new Text();
	IntWritable v  = new IntWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		
		String [] spilts = line.split(" ");
		for (String word : spilts) {
			
			//分割后，将1设置为value
			k.set(word);
			context.write(k, v);
		}
	
	}
}
