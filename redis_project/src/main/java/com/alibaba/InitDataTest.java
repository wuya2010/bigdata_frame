package com.alibaba;

import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;

public class InitDataTest {
	
	// http://192.168.3.165:8080/MySeckill/index.jsp
	// http://192.168.2.153:8080/MySeckill/index.jsp
	

	// 初始化库存的方法
	@Test
	public void test() {
		
		Jedis jedis =new Jedis("192.168.25.128",6379);
		
		System.out.println(jedis.ping());
		
		String productKey="sk:"+1001+":product";
		String userKey="sk:"+1001+":user";
		
		jedis.set(productKey, "500");
		
		jedis.del(userKey);
		
		String string = jedis.get(productKey);
		
		System.out.println(string);
		
		jedis.close();
	}
	
	@Test
	public void test1() {
		
		Jedis jedis =new Jedis("192.168.25.128",6379);
		
		System.out.println(jedis.ping());
		
		String qtkey="sk:"+1001+":qt";
		String usersKey="sk:"+1001+":usr";
		
		jedis.set(qtkey, "300");
		jedis.set(usersKey, "500");

//		jedis.del(usersKey);
		
		String string = jedis.get(qtkey);

		System.out.println(string);
		
		jedis.close();
	}

	@Test
	public void getTest(){
		try {

			SecKill_redis.doSecKill("1002","1002");

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

}
