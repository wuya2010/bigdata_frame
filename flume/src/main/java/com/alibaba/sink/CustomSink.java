package com.alibaba.sink;


import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


//继承一个abstratsink   实现一个接口
public class CustomSink extends AbstractSink implements Configurable{

	//在前面声明
	private Integer batchSize =null;
	private String prefix = null;
	
	//组个往集合中放
 	private List<Event> events  = new ArrayList<>();
 	//获取名字
 	//导包：  slf 4j 下的包
 	//给每一个对象 起一个名字 （传入一个类名）            ，方法s生成一个自己的log对象
 	private static Logger LOG = LoggerFactory.getLogger(CustomSink.class);
 	
	
	//从channel拿数据，循环执行
	@Override
	public Status process() throws EventDeliveryException {
		
		//启动之前清除   ，最后加进入的  ， 写完之后再考虑考虑
		events.clear();
		
		//获取配置文件中的channel， 从哪个channle获取数据
		Channel channel = getChannel();
		//获得事务对象  ，  开启事务
		Transaction tx = channel.getTransaction();
		
		//开始事务
		tx.begin();
		
		//从channel中拿数据  dotake() ,  这里实际时take方法
		try {
			//一次拿一批
			for (int i = 0; i < batchSize; i++) {
				
				Event event = channel.take();
				
				if (event ==null ) {
					break;
				}
				events.add(event);	
			}
			
			
			//logger4j 来显示
			for (Event event : events) {
				
				//info： 级别  
			
				LOG.info(prefix+"-"+new String(event.getBody()));
				//打印处理： atguigu（前缀）-aaa(输入的)
			}
			 tx.close();
			
		} catch (Exception e) {
			
			//事务rollback（）：回滚
			tx.rollback();
			return Status.BACKOFF;
		}
		
		
		return Status.READY;
		
		
	}

	
	
	
	@Override
	public void configure(Context context) {

		//首先获取batchsize  与 prefix
		batchSize  = context.getInteger("batchSize");
		prefix = context.getString("prefix");
		
	}

	
	
	
	
	
}
