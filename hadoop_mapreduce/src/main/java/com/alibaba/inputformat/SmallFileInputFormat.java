package com.alibaba.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 自定义InputFormat
 *   1. 继承 FileInputFormat类
 *   2. 重写createRecordReader 方法
 *   3. 根据实际情况，重写isSplitable方法， 指定读取的文件是否可切割
 */
public class SmallFileInputFormat  extends FileInputFormat<Text, BytesWritable> {
	
	/**
	 * 我们希望将读取的一个文件内容完整的写到 sequenceFile中，所有我们不允许文件被切割,因此返回false.
	 */
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		
		return false;
	}

	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		//创建RecordReader对象.
		SmallFileRecordReader  recordReader  =  new SmallFileRecordReader() ;
		
		//调用初始化方法
		recordReader.initialize(split, context);
		
		return recordReader;
	}

	
}
