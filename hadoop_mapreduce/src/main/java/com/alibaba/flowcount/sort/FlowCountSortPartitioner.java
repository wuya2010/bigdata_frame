package com.alibaba.flowcount.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowCountSortPartitioner extends Partitioner<FlowBean, Text> {
	
	@Override
	public int getPartition(FlowBean key, Text value, int numPartitions) {
		
		
		// 获取手机号的前3位
		String keyStr = value.toString();
		String phone_pre_three =  keyStr.substring(0,3);
//		System.out.println("keyStr:" + keyStr);
//		System.out.println("phone_pre_three:" + phone_pre_three) ;
//		
		//判断
		int partition = 4 ;
		
		if("136".equals(phone_pre_three)) {
			partition  = 0 ; 
		}else if("137".equals(phone_pre_three)) {
			partition = 1;
		}else if("138".equals(phone_pre_three)) {
			partition = 2 ; 
		}else if ("139".equals(phone_pre_three)) {
			partition = 3; 
		}
		return partition;
		
	}

}
