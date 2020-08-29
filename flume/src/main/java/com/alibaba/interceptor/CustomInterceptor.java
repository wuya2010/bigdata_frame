package com.alibaba.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

public class CustomInterceptor  implements Interceptor  {

	//初始时调用
    @Override
    public void initialize() {//com.atguigu.flume.interceptor.CustomInterceptor

    }

    // fixme: 对于 evnet 的判断
    @Override
    public Event intercept(Event event) {
    	
    	//==> public byte[] getBody();
        //fixme : Returns the raw byte array of the data contained in this event
        byte[] body = event.getBody();

        //先进行判断 todo: body[0]  ????
        if (body[0] >= '1' && body[0] <= '9') {
        	
        	
        	// public void setHeaders(Map<String, String> headers);
        	// ==>public Map<String, String> getHeaders();
        	//fixme:  Returns a map of name-value pairs describing the data stored in the body.
        	//头文件中put
            event.getHeaders().put("type", "number");
            
        } else if (body[0] >= 'a' && body[0] <= 'z') {
        	
            event.getHeaders().put("type", "letter");
        }

       // new String(body, charset.forName("UTR-8"));
        return event;
    }

    
    //fixme: event 的集合
    @Override
    public List<Event> intercept(List<Event> events) {

    	
    	//是否需要建立一个events集合
        for (Event event : events) {
            intercept(event);
        }
        
        //这里理解一些？
        return events;

    }

    
    //agent推出时调用
    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new CustomInterceptor();
        }

        	//获取代码中的参数
        @Override
        public void configure(Context context) {
        		
        }
    }
	
}
