package com.alibaba;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

// 当服务器上线以后，将相关的数据注册到Zookeeper上.
public class Server {
	
	private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
	
	private int sessionTimeout = 10000 ;
	
	private ZooKeeper zkClient ;
	

	private String parentPath = "/servers";
	
	public static void main(String[] args)  throws Exception{
		
		Server server = new Server();
		
		//1. 初始化， 获取到Zookeeper的客户端对象
		server.init();
		
		//2. 判断指定的路径是否存在，如果不存在，创建=====》判断节点是否存在
		server.checkPathIsExists();
		
		//3. 将当前服务器相关的数据注册到对应的路径下====>给节点servder赋值
		server.regist(args[0]);
		
		//4. 当前服务器的业务处理
		server.doBusiness();
	}

	private void doBusiness() throws Exception{
		//方法很危险，尽量少用====》为什么要睡呢？保持业务得连接，断了之后就没有数据了
		Thread.sleep(Long.MAX_VALUE);
	}

	private void regist(String data ) throws Exception {
		String path = 
				zkClient.create(parentPath+"/server", data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		
		System.out.println(path +  " is on line .");
	}

	private void checkPathIsExists() throws Exception{
		Stat stat = zkClient.exists(parentPath, false);
		if(stat == null ) {
			//节点的值,创建这个接待你
			zkClient.create(parentPath, "servers".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}

	private void init() throws Exception {
		zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				
			}
		});
	}
}
