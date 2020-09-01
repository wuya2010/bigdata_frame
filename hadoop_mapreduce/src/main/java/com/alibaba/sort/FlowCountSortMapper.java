package com.alibaba.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
	
	FlowBean k = new FlowBean();
	Text v = new Text();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context)
			throws IOException, InterruptedException {
		
		//获取一行数据: 13470253144	180	180	360
		String line  = value.toString();
		String [] fields = line.split("\t");
		
		//封装value
		v.set(fields[0]);
		//封装key
		k.setUpFlow(Long.parseLong(fields[1]));
		k.setDownFlow(Long.parseLong(fields[2]));
		k.setSumFlow(Long.parseLong(fields[3]));
		
		//写出
		context.write(k, v);
	}
}
