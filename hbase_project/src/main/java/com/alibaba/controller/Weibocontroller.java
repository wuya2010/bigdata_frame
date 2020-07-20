package com.alibaba.controller;


import com.alibaba.service.WeiboService;

/** 1. 初始化     init()
* 2.发微博        publish()
* 3.关注默认    follow()
* 4.取消关注     unfollow()
* 5.通过rowkey前缀：  getCellsByPrefix
* 6.获取最新的微博：  getAllRecentWeibos*/



//在这里调用方法，需要实现什么： 总的思路在这里，具体实现再往下走


//分析完成了，再动手写，这也是一个思路
//第一步：  就在这里把所有需要实现的东西列处理，写出类名，需要的参数



public class Weibocontroller {
	
	private WeiboService service =  new WeiboService();

	public void init() throws Exception{
		// 创建命名空寂以及表名的而定义
		//创建微博内容表
		//创建用户关系表
		//创建微博内容跟接受邮件表
		//======》 这些内容全部在 server类中完成
		service.init();
	}

	
	
	

	
	//5) 发布微博内容
	public  void publish(String star, String context) throws Exception {
		
		service.publish(star, context);
		
	}
	
	

	
   //6) 添加关注用户
	public void follow(String star, String fans) throws Exception {
		service.follow(star, fans);
	}
	
	
	
	
	//7) 移除（取关）用户
	public void unfollow(String star,String fans) throws Exception {
		service.unfollow(star, fans);
	}
	
	
	
	
	
	
	//8) 获取关注的人的微博内容
	//8.1 获取某个star的所有weibo==>通过rowkey前缀：  getCellsByPrefix
	public void getCellsByPrefix(String tableName, String prefix, String family, String column) throws Exception {
		
		service.getCellByPrefix(tableName, prefix, family, column);
	}

	
	
	
	
	
	//8.2 获取某个fans的所有star的近期weibo==>getAllRecentWeibos
	public void getAllRecentWeibos(String fans) throws Exception {
		
		service.getAllRecentWeibos(fans);
	}
	
	
	
	
}
