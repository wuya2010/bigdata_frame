package com.alibaba.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SmallFileInputFormatMapper  extends Mapper<Text, BytesWritable, Text, BytesWritable> {
	
	@Override
	protected void map(Text key, BytesWritable value, Mapper<Text, BytesWritable, Text, BytesWritable>.Context context)
			throws IOException, InterruptedException {
		// key: 文件的路径
		// value: 文件的内容
		
		context.write(key, value);
	}
}	
