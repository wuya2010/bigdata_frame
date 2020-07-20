package com.alibaba.outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogFilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		//解决最终写到文件中没有换行的问题
		String line = value.toString();
		
		line = line+"\n";
		
		k.set(line);
		context.write(k, NullWritable.get());
	}
}
