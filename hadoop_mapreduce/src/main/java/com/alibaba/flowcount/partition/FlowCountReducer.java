package com.alibaba.flowcount.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
	FlowBean v  = new FlowBean();
	
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		//汇总
		int totalUpFlow = 0 ; 
		int totalDownFlow = 0 ; 
		int totalSumFlow = 0 ;
		
		for (FlowBean flowBean : values) {
			totalUpFlow += flowBean.getUpFlow();
			totalDownFlow += flowBean.getDownFlow();
			totalSumFlow += flowBean.getSumFlow();
		}
		
		v.setUpFlow(totalUpFlow);
		v.setDownFlow(totalDownFlow);
		v.setSumFlow(totalSumFlow);
		
		context.write(key, v);
	}
}
