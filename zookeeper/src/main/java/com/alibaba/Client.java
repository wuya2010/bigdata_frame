package com.alibaba;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

// 监听Zookeeper注册的服务器的上下线，实时获取到当前的可用服务器节点。
public class Client {
	private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
	
	private int sessionTimeout = 10000 ;
	
	private ZooKeeper zkClient ;
	
	private String parentPath = "/servers";
	
	
	public static void main(String[] args)  throws Exception{
		Client client = new Client();
		//1. 初始化， 获取到Zookeeper客户端对象
		client.init();
		//2. 获取当前可用的服务器的节点， 并监听 .
		List<String> servers = client.getServers();
		System.out.println("当前可用的节点为: " + servers);
		//3. 客户端的业务处理
		client.duBusiness();
		
	}

	private void duBusiness() throws Exception {

		Thread.sleep(Long.MAX_VALUE);
	}

	
	//所谓监听就是获取子节点的变化
	private List<String > getServers()  throws Exception{
		
		List<String> nodes = zkClient.getChildren(parentPath, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				try {
					//实时获取新的服务器节点信息
					List<String> newNodes = getServers();
					System.out.println("当前可用的新节点为: " + newNodes);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		
		return nodes ;
	}

	private void init()  throws Exception{
		zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				
			}
		});
	}
}
