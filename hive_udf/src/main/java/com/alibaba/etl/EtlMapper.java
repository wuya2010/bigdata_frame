package com.alibaba.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EtlMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		Text k = new Text();
	
		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws java.io.IOException ,InterruptedException {
			
			String line = value.toString();
			
			String resultline = EtUtils.etData(line);
			
			if (resultline==null) {
				
				return;
			}
			
			k.set(resultline);
			
			context.write(k, NullWritable.get());
		};

}
