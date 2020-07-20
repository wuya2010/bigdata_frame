package com.alibaba.flowcount.partition;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *  描述 上行流量  下行流量  总流量
 *  
 *  因为要写到磁盘，因此该类要实现Hadoop的序列化接口
 */
public class FlowBean implements Writable {
	
	private long upFlow ;
	private long downFlow; 
	private long sumFlow;
	
	/**
	 * 空参构造器
	 */
	public FlowBean() {
	}
	
	/**
	 * 序列化方法 
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}
	/**
	 * 反序列化方法
	 * 
	 * 注意: 反序列化的顺序要与序列化的顺序一致.
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		upFlow = in.readLong();
		downFlow = in.readLong();
		sumFlow = in.readLong();
	}
	
	public long getUpFlow() {
		return upFlow;
	}
	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}
	public long getDownFlow() {
		return downFlow;
	}
	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}
	public long getSumFlow() {
		return sumFlow;
	}
	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}
	@Override
	public String toString() {
		return  upFlow +"\t" + downFlow +"\t" +sumFlow; 
	}

	public void set(long upFlow, long downFlow) {
		this.upFlow = upFlow; 
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}

}
