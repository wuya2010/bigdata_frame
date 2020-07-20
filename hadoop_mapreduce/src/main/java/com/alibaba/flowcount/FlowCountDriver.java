package com.alibaba.flowcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FlowCountDriver {
	
	public static void main(String[] args) throws Exception {
		
		args = new String[] {"d:/input/inputflow","d:/output5"};
		
		//1. 获取Job
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		//2. 关联jar
		job.setJarByClass(FlowCountDriver.class);
		
		//3. 关联Mapper 和 Reducer
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		//4. 设置Map输出的key和value的类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		//5. 设置最终输出的key和value的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//6. 设置输入和输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//7. 提交Job
		job.waitForCompletion(true);
		
	}
}
