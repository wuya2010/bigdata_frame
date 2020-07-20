package com.alibaba.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class ReadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{

	//实现将HDFS中的数据写入到HBase表中
	
	protected void map(LongWritable key, Text value, Context context)
			throws IOException ,InterruptedException {
		
		String line = value.toString();
		
		String[] split = line.split("\t");
		
		//为什么要做这个判断
		if (split.length < 3 ) {
			return;
		}
		
		Put put = new Put(Bytes.toBytes(split[0]));
		
		//获取一列的数据==。 Put addColumn(byte [] family, byte [] qualifier, byte [] value)
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), 
				Bytes.toBytes(split[2]));
		//获取
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), 
				Bytes.toBytes(split[2]));
		
		context.write(new ImmutableBytesWritable(Bytes.toBytes(split[0])), put);
		
	};
	
	
	
		
}
