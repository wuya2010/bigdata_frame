package com.alibaba.interceptor2;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;


//==》com.atguigu.flume.interceptor.LogETLInterceptor
public class LogETLInterceptor implements Interceptor{

	@Override
	public void initialize() {

	}


	//定义拦截器
	@Override
	public Event intercept(Event event) {
		
		byte[] body = event.getBody();


		//fixme: 建立new String
		String log = new String(body, Charset.forName("utf-8"));
		
		
		//判断头文件
		//"en":"start"
		/*
		 * 1566563097472|{"cm":{"ln":"-58.8","sv":"V2.6.4","os":"8.2.4","g":"8GQ7R5N2@gmail.com","mid":"0","nw":"4G","l":"pt","vc":"8","hw":"640*1136","ar":"MX","uid":"0","t":"1566542979959","la":"22.5","md":"Huawei-3","vn":"1.0.4","ba":"Huawei","sr":"F"},"ap":"app",
		 * "et":[{"ett":"1566467054482","en":"display","kv":{"goodsid":"0","action":"1","extend1":"1","place":"2","category":"20"}},{"ett":"1566556294724","en":"newsdetail","kv":{"entry":"1","goodsid":"1","news_staytime":"6",
		 * "loading_time":"0","action":"3","showtype":"0","category":"19","type1":"325"}},{"ett":"1
		 */
		if (log.contains("start")) {
			if (LogUtils.validateStart(log)) {
				return event;//这个逻辑有问题？
			}
			
		}else {
			   if (LogUtils.validateEvent(log)){
	                return event;
	            }
			
		}
		
		return null;
	}

	
	
	@Override
	public List<Event> intercept(List<Event> events) {
		
		List<Event> interceptors = new ArrayList<>();
		
		for (Event event : events) {
			
			//向下转型 todo： intercept
			Event interceptor1 =intercept(event);
			
			if (interceptor1 != null) {
			//list的集合
				interceptors.add(interceptor1);
			}
			
		}
		return null;
	}

	
	@Override
	public void close() {

	}
		
	 public static class Builder implements Interceptor.Builder {

		@Override
		public void configure(Context context) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public Interceptor build() {
			
			return new LogETLInterceptor();
		}
	
	 }
		
}
