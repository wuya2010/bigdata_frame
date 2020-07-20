package com.alibaba.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HRegionPartitioner;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class Driver {

	public static void main(String[] args) throws Exception {
		
		
		Configuration conf = HBaseConfiguration.create();
		
		conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(Driver.class);
		
		job.setMapperClass(ReadMapper.class);
		
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		
		FileInputFormat.setInputPaths(job, new Path("/input_fruit"));
		
		job.setNumReduceTasks(100);
		
		TableMapReduceUtil.initTableReducerJob("fruit_mr", WriteReducer.class,
				job, HRegionPartitioner.class);
		
		
		 job.waitForCompletion(true);
		
		
		
	}
	
}
