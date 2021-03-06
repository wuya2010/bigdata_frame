package com.alibaba.group;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer  extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
	
	@Override
	protected void reduce(OrderBean key, Iterable<NullWritable> values,
			Reducer<OrderBean, NullWritable, OrderBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
		
		context.write(key, NullWritable.get());
		
		/*for (NullWritable nullWritable : values) {
			
			context.write(key, NullWritable.get());
		}*/
	}
}
