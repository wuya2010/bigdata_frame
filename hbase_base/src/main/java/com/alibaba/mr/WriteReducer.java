package com.alibaba.mr;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


//==》TableReducer<KEYIN, VALUEIN, KEYOUT>
//extends Reducer<KEYIN, VALUEIN, KEYOUT, Mutation>
public class WriteReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable>{

		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<Put> values,
				Reducer<ImmutableBytesWritable, Put, NullWritable, Mutation>.Context context)
				throws IOException, InterruptedException {

		
			for (Put put : values) {
				
				context.write( NullWritable.get(),put);
				
			}
		
		
		
		
		}
	
	
}
