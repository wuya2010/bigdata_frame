package com.alibaba.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean  implements WritableComparable<FlowBean> {
	
	private long upFlow ;
	private long downFlow;
	private long sumFlow ;
	
	public FlowBean() {
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
		return  upFlow + "\t" + downFlow + "\t" + sumFlow ;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		upFlow =in.readLong();
		downFlow =in.readLong();
		sumFlow =in.readLong();
	}
	@Override
	public int compareTo(FlowBean o) {
		//因为要排序，因此要支持比较。 要写具体的比较规则
		//比较规则: 按照总流量进行倒叙
		
		int result ;
		if(this.sumFlow > o.getSumFlow()) {
			result = -1; 
		}else if (this.sumFlow  < o.getSumFlow()) {
			result = 1 ; 
		}else {
			result = 0 ; 
		}
		return result ;
	}
	
}
