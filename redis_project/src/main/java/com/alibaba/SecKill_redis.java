package com.alibaba;

import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.io.IOException;
import java.util.List;


/*
 * 每个用户限定秒杀一件！
 * 
 * 数据模型：
 * 		在秒杀中获取到了userid,productId
 * 
 * 		产品库存：
 * 					key： 和productId相关
 * 					value:  string
 * 
 *		某件商品秒杀成功的用户名单：
 *					
 *					key： 和productId相关
 *					value：  set
 * 		
 * 业务流程：
 * 		① 查询当前用户是否已经成功秒杀过
 * 				jedis.ismember();
 * 			用户已经存在，return false
 * 		
 * 		② 查询库存是否合法
 * 				jedis.get();
 * 			null:  商家尚未上架，return false;
 * 			>0 :  可秒杀
 * 		③  秒杀流程
 * 				将当前用户加入到成功秒杀名单中；
 * 					jedis.sadd();
 * 
 * 				库存减1
 * 					jedis.decy();
 * 				return true;
 * 
 * 1.压力测试
 * 		ab(apache benchmark):  Centos 自带
 * 			-n requests    ： 发送的请求数
    		-c concurrency ： 并发量 
    		-p postfile   ： 发送Post请求，需要把参数附加在一个文件中，结合-T一起使用
    		-T ：发送post需要设置为application/x-www-form-urlencoded
    		
    、	ab -n 2000 -c 300  -p  /root/postarg -T 'application/x-www-form-urlencoded' http://192.168.3.165:8080/MySeckill/doseckill

 * 2. 超卖问题
 * 		高并发条件下，容易出现超卖问题！
 * 
 * 3.  Java层面，在线程上加synchronized代码块
 * 			悲观锁，效率低，保证安全，不公平！
 * 
 * 4.  使用Redis解决超卖问题
 * 			核心：加锁！使用Redis加锁！
 * 
 * 			在第一次使用key之前，执行watch命令！
 * 
 * 			redis的乐观锁，存在资源的浪费，存在不公平！
 * 
 * 
 * 5. 解决秒杀中的不公平
 * 
 * 			哪个请求先发送，先秒杀成功！
 * 			使用Lua脚本！
 * 
 * 6. Lua脚本可以调用redis的函数。
 * 	  Lua脚本可以编写一个脚本，在脚本中，执行批量的命令。将脚本一次性提交给redis服务器执行，类似redis中的事务
 * 
 * 	区别： 原子性
 * 
 * 				事务                                                 lua
 * 			
 * 			开启事务块multi          编写一个脚本文件
 * 				命令1                                        调用redis的函数1
 * 				命令2                                         调用redis的函数2
 * 				...                   ...
 
 * 				exec(提交并执行)        提交脚本
 * 
 * 
 *            原子是命令                                     原子是整个脚本
 * 
 * 
 *          T1     T2
 *     
 * 0.01    133     132
 * 
 * 
 * 0.05    175     176
 * 
 * 					发送事务的执行
 * 
 * 			
 * 
 * 
 * 
 * 
 * 
 * 			
 * 			
 * 				
 * 
 */

public class SecKill_redis {

	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SecKill_redis.class);

	// 秒杀的实际方法
	public static boolean doSecKill(String uid, String prodid) throws IOException {
		
		// 准备key
		String productKey="sk:"+prodid+":product";
		String userKey="sk:"+prodid+":usr";
		
		//获取Jedis实例
		JedisPool jedisPool = JedisPoolUtil.getJedisPoolInstance();
		
		Jedis jedis = jedisPool.getResource();
		
		// 判断用户是否秒杀过
		if (jedis.sismember(userKey, uid)) {
			
			System.err.println(uid+"已经秒杀过！");
			
			jedis.close();
			
			return false;
			
		};
		
		// 为库存加锁
		jedis.watch(productKey);
		
		// 检查库存
		String store = jedis.get(productKey);
		
		if (store==null) {
			
			System.err.println("商家尚未上架"+prodid+" 产品！");
			
			jedis.close();
			
			return false;
			
		}
		
		int store_int = Integer.parseInt(store);
		
		// 没有库存
		if (store_int<=0) {
			
			System.err.println(prodid+"已经秒完！");
			
			jedis.close();
			
			return false;
			
		}
		
		//------------------秒杀流程---------------------
		
		//开启事务
		// 加锁并开启事务： 	===》 jedis.watch(productKey);
		Transaction transaction = jedis.multi();
		
		// 将当前用户加入到成功秒杀名单中
		transaction.sadd(userKey, uid);
		
		// 库存减1
		transaction.decr(productKey);
		
		// 执行
		List<Object> result = transaction.exec();

		// 数量太少不进行秒杀
		if (result==null || result.size() <2) {
			
			System.out.println(uid+"秒杀失败！");
			
			jedis.close();
			
			return false;	
			
		}
		
		
		System.out.println(uid+"成功秒杀到了产品！");
		
		jedis.close();
		
		return true;	
		
	}

}
