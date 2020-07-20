package com.alibaba.table;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<TableBean> values,
                          Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
	    
		List<TableBean>  orders = new ArrayList<>();
		TableBean  pd = new TableBean() ;
		
		for (TableBean tableBean : values) {
			
			if("order".equals(tableBean.getFlag())) {
				try {
					//来自于order表的数据
					TableBean order  = new TableBean();
					
					//将属性值从原始bean复制到目标bean
					BeanUtils.copyProperties(order, tableBean);
					orders.add(order);
				} catch (Exception e) {
				}
					
			}else {
				try {
					//来自于pd表的数据
					BeanUtils.copyProperties(pd, tableBean);
				} catch (Exception e) {
					
				}
			}
		}
		
		// join 
		// 将pd对象中的pname属性值 赋值给 从orders集合中迭代出的每个TableBean对象的pname属性上.
		for (TableBean tableBean : orders) {
			
			tableBean.setPname(pd.getPname());
			
			context.write(tableBean, NullWritable.get());
		}
		
	}
}
