package com.alibaba.table2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class Table2Driver {
	public static void main(String[] args) throws Exception {

		// 0 根据自己电脑路径重新配置
		args = new String[] { "d:/input/inputtable2", "d:/output1" };

		// 1 获取job信息
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		// 2 设置加载jar包路径
		job.setJarByClass(Table2Driver.class);

		// 3 关联map
		job.setMapperClass(Table2Mapper.class);

		// 4 设置最终输出数据类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// 5 设置输入输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 6 加载缓存数据
		job.addCacheFile(new URI("file:///d:/input/inputcache/pd.txt"));

		// 7 Map端Join的逻辑不需要Reduce阶段，设置reduceTask数量为0
		job.setNumReduceTasks(0);

		// 8 提交
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}

}
