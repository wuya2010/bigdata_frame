package com.alibaba.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HRegionPartitioner;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

public class Driver {

	public static void main(String[] args) throws Exception {
		
		//传入hbase的conf
		Configuration conf = HBaseConfiguration.create();
		//加入zookeeper的参数  ==》端口号配不配都行
		conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
		
		//获取实例
		Job job = Job.getInstance(conf);
		
		//将jar包提共给临时路径
		job.setJarByClass(Driver.class);
		
		//创建一个扫描操作的对象，这里不用加范围，整表
		Scan scan = new Scan();
		
		//数据类型： scans, mapper, outputKeyClass, outputValueClass, job
		//用scan的方式读数据
        TableMapReduceUtil.initTableMapperJob("fruit", 
        		scan, ReadMapper.class, ImmutableBytesWritable.class,
				Put.class, job);
		
        
	
		//这里设置的reducer数一定要大
		job.setNumReduceTasks(100);
		
		//===>table, reducer, job, partitioner
		TableMapReduceUtil.initTableReducerJob("fruit_mr", WriteReducer.class, 
				job, HRegionPartitioner.class);
		
		 job.waitForCompletion(true);
		
		
		
	}
	
}
