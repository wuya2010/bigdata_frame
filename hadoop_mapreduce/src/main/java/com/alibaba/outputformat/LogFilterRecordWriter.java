package com.alibaba.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogFilterRecordWriter extends RecordWriter<Text, NullWritable> {
	
	FSDataOutputStream atguiguOut  ;
	FSDataOutputStream otherOut ;
	TaskAttemptContext context ;
	
	public LogFilterRecordWriter(TaskAttemptContext context) {
		this.context = context ;
		
		 try {
			 FileSystem fs =  FileSystem.get(context.getConfiguration());
			 atguiguOut = fs.create(new Path("d:/outputlog/atguigu.log"));
			 otherOut = fs.create(new Path("d:/outputlog/other.log"));
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 核心业务逻辑
	 *  将包含"atguigu"的日志数据写出到 atguigu.log , 其他的写出到  other.log
	 */
	@Override
	public void write(Text key, NullWritable value) throws IOException, InterruptedException {
//		  atguiguOut = 
//				new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("d:/atguigu.log")),"utf-8"));
//		  otherOut = 
//				new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("d:/other.log")),"utf-8"));
		  String line = key.toString();
		  if(line.contains("atguigu")) {
			  //写入到atguigu.log
			  atguiguOut.write(line.getBytes());
		  }else {
			  //写入到other.log
			  otherOut.write(line.getBytes());
		  }
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		 	IOUtils.closeStream(atguiguOut);
			IOUtils.closeStream(otherOut);
	}

}
