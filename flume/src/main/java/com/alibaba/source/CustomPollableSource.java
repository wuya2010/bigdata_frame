package com.alibaba.source;


import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractPollableSource;

import java.util.ArrayList;
import java.util.List;

//poll:可以自动给循环获取数据
public class CustomPollableSource  extends AbstractPollableSource{

	private Integer batchSizee =null; 
	private String  prefix= null; 
	//提出来的主体结构
	private List<Event> events = new ArrayList<>();
 	
	
	//会反复调用获取数据，最重要的==》3步
	@Override
	protected Status doProcess() throws EventDeliveryException {
		
		
		//读取数据，并封装成Event   ==》理解这个过程的逻辑
		try {
			
			//相当于一批数据 batchsize
			for (int i = 0; i < batchSizee; i++) {
				
				String message = prefix + "-" + i;
				//EventBuilder.withBody(body)
				Event event = EventBuilder.withBody(message.getBytes());
				
				events.add(event);
				
			}
			
			//将event交给channelProcessor
			ChannelProcessor channelProcessor = getChannelProcessor();		
			
			//这里传入一批events数据
			channelProcessor.processEventBatch(events);
			
			//每次放完后需要清空
			events.clear();
			
		} catch (Exception e) {
			
			events.clear();
			//然会backoff，表示处理过程中出现问题 ，等一会再来处理
			return Status.BACKOFF;
		}
			//返回ready ,表示这批数据处理完
			return Status.READY;
	}

	//获取配置 ==》2步
	@Override
	protected void doConfigure(Context context) throws FlumeException {
		batchSizee	= context.getInteger("batchSize",20);
		//设置前缀
		prefix = context.getString("prefix");
		
	}

	
	//初始化连接==》  1步
	@Override
	protected void doStart() throws FlumeException {
		System.out.println("*******************************");
        System.out.println("custom source start");
        System.out.println("*******************************");
		
	}

	
	//关闭： 最后会打印处理
	@Override
	protected void doStop() throws FlumeException {
		  System.out.println("*******************************");
	        System.out.println("custom source stop");
	        System.out.println("*******************************");
		
	}

	
	
	
}
