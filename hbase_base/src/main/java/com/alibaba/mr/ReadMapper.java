package com.alibaba.mr;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


//Hbase提供的专门的mapper
// TableMapper继承了extends Mapper<ImmutableBytesWritable, Result, KEYOUT, VALUEOUT>
//这里把输入写死了           map序列化器： habse提供了put的序列化器
public class ReadMapper extends TableMapper<ImmutableBytesWritable, Put>{

	//将HBase中的fruit表中的一部分数据（将name列迁移），通过MR迁入到HBase中的另一个表：fruit_mr表中
	
	//===>首先从Hbase中获取数据
	//==》key ： rowkey    value：  指的是具体的值
	@Override    
	protected void map(ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context)
			throws IOException, InterruptedException {
		
		//一次读取一行的数据===》获取keyrow
		Put put = new Put(key.get());
		
		//返回单元格数组==>rawCells()
		Cell[] cells = value.rawCells();
		
		for (Cell cell : cells) {
			
			//获取列名： cloneQualifier
			if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
				
				//===》kv.getRowArray(), kv.getRowOffset(), kv.getRowLength());
				//==》add方法更方便
				put.add(cell);
			}
			
		}
		
		//将key和put放入
		context.write(key, put);
	
	}
	
	
		
}
