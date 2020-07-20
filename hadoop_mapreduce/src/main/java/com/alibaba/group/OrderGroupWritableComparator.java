package com.alibaba.group;

import org.apache.hadoop.io.WritableComparator;

public class OrderGroupWritableComparator extends WritableComparator {
	
	public OrderGroupWritableComparator() {
		super(OrderBean.class,true);
	}
	
	@Override
	public int compare(Object  a, Object b) {
		// a 和 b 实际上就Map端输出的key.
		OrderBean aBean =(OrderBean)a; 
		OrderBean bBean =(OrderBean)b; 
		
		int result ; 
		if(aBean.getOrder_id() > bBean.getOrder_id()) {
			result = 1; 
		}else if(aBean.getOrder_id() < bBean.getOrder_id()) {
			result = -1 ;
		}else {
			result = 0 ;
		}
		
		return result ;
	}
}
