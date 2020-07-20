package com.alibaba.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

public class CustomInterceptor2  implements Interceptor  {

    @Override
    public void initialize() {//com.atguigu.flume.interceptor.CustomInterceptor

    }

    
    @Override
    public Event intercept(Event event) {

        byte[] body = event.getBody();

        if (body[0] >= '1' && body[0] <= '9') {
        	
            event.getHeaders().put("topic", "number");
            
        } else if (body[0] >= 'a' && body[0] <= 'z') {
        	
            event.getHeaders().put("topic", "letter");
        }

        return event;
    }

    
    
    @Override
    public List<Event> intercept(List<Event> events) {

        for (Event event : events) {
            intercept(event);
        }

        return events;

    }

    
    
    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new CustomInterceptor2();
        }

        @Override
        public void configure(Context context) {

        }
    }
	
}
