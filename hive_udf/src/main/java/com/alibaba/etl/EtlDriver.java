package com.alibaba.etl;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class EtlDriver {

	public static void main(String[] args) throws Exception {
		
		
		//前正在运行的Java虚拟机，这个status表示退出的状态码，非零表示异常终止。
		
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(EtlDriver.class);
		
		job.setMapperClass(EtlMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		
		job.setNumReduceTasks(0);
		
		

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);	
		
		System.exit(result?0:1);
		
	}
	
}
