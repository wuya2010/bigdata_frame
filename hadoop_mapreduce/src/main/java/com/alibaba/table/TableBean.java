package com.alibaba.table;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableBean  implements Writable {
	
	private String order_id ; // 订单id
	
	private String pid ; // 产品id
	
	private String pname ; // 产品名
	
	private String flag ; // 标签
	
	private int amount ;
	
	public TableBean() {
	}

	public String getOrder_id() {
		return order_id;
	}

	public void setOrder_id(String order_id) {
		this.order_id = order_id;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public String getPname() {
		return pname;
	}

	public void setPname(String pname) {
		this.pname = pname;
	}

	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	@Override
	public String toString() {
		return order_id + "\t" + pname +"\t" + amount;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(order_id);
		out.writeUTF(pid);
		out.writeUTF(pname);
		out.writeUTF(flag);
		out.writeInt(amount);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		order_id = in.readUTF();
		pid = in.readUTF();
		pname = in.readUTF();
		flag= in.readUTF();
		amount = in.readInt();
	}
	
	
}
