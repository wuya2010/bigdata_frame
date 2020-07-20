package com.alibaba.nline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

public class NLineInputFormatDriver {
	public static void main(String[] args)
			throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

		// 输入输出路径需要根据自己电脑上实际的输入输出路径设置
		args = new String[] { "d:/input/inputnline", "d:/output4" };

		// 1 获取job对象
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		// 7设置每个切片InputSplit中划分三条记录
		NLineInputFormat.setNumLinesPerSplit(job, 3);

		// 8使用NLineInputFormat处理记录数
		job.setInputFormatClass(NLineInputFormat.class);

		// 2设置jar包位置，关联mapper和reducer
		job.setJarByClass(NLineInputFormatDriver.class);
		job.setMapperClass(NLineInputFormatMapper.class);
		job.setReducerClass(NLineInputFormatReducer.class);

		// 3设置map输出kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 4设置最终输出kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 5设置输入输出数据路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 6提交job
		job.waitForCompletion(true);
	}

}
