package com.alibaba.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
		
		FlowBean outV = new FlowBean();
		Text outK  = new Text();
	
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			//1. 获取一行 例如:  7 	13560436666	120.196.100.99		1116		 954			200
			String line = value.toString();
			
			//2. 切割
			String[] split = line.split("\t");
			
			//处理数据
			String k = split[1];
			outK.set(k);
			
			String upFlow = split[split.length-3];
			String downFlow = split[split.length-2];
			outV.set(Long.parseLong(upFlow),Long.parseLong(downFlow));
			context.write(outK, outV);
		}
}
