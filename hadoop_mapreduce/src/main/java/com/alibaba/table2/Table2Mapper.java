package com.alibaba.table2;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class Table2Mapper  extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	Map<String,String> pdMap = new HashMap<>();
	
	Text k = new Text();
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		//获取缓存文件
		URI[] cacheFiles = context.getCacheFiles();
		Path path  = new Path(cacheFiles[0].getPath());
		FileSystem fs  = FileSystem.get(context.getConfiguration());

		FSDataInputStream fis = fs.open(path);
		BufferedReader br  = new BufferedReader(new InputStreamReader(fis,"utf-8"));
		String line ;
		while(( line = br.readLine())!=null) {
			// 01	小米
			String [] fields = line.split("\t");
			pdMap.put(fields[0], fields[1]);
		}
		//关闭流
		br.close();
	}





	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		// 读取一行    1001	01	1
		String line = value.toString();
		
		String [] fields = line.split("\t");
		fields[1] = pdMap.get(fields[1]);
		
		String keyStr = fields[0] +"\t" +  fields[1] + "\t" + fields[2];
		
		k.set(keyStr);
		
		context.write(k, NullWritable.get());
	}
}
