package com.alibaba;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class TestZookeeper {
	
	
//	private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
	private String connectString = "192.168.25.128:2181";
	// minSessionTimeout=4000  maxSessionTimeout=40000
	private int sessionTimeout = 10000;
	
	private ZooKeeper zkClient ; 
	
	
	/**
	 * 判断Znode是否存在
	 */
	@Test
	public void checkIsExists() throws  Exception {
		
		Stat stat = zkClient.exists("/alibaba", false);
		
		if(stat ==null ) {
			System.out.println("Znode不存在");
		}else {
			System.out.println("Znode存在");
		}
	}
	
	/**
	 * 修改Znode的数据
	 */
	@Test
	public void setZnodeData() throws Exception{
		
		//version这里取：获取的信息中dataversion为0 ，这里不做修改
		Stat stat = zkClient.setData("/alibaba", "sgg".getBytes(), 0);
		
		//12884901924,12884901938,1564399424820,1564400250201,2,0,0,0,3,0,12884901924
		System.out.println(stat);
	}
	
	/**
	 * 获取Znode的数据
	 */
	@Test
	public void getZnodeData() throws Exception {
		
		//false:是否要监视
		//null:是否要状态信息，这里给null
		byte [] datas = zkClient.getData("/sangguo", true, null);
		
		System.out.println(new String(datas));
		
	}
	
	/**
	 * 获取子Znode
	 */
	@Test
	public void  listSubZnode()  throws Exception{
		
//		List<String> nodes  = zkClient.getChildren("/", true);
//		
//		System.out.println(nodes);
		
		//对于这个watch的理解， 当客户端watch的节点发送变化，触发watch
		List<String> nodes = zkClient.getChildren("/", new Watcher() {

			// 获取所有的子节点： [alibaba, sangguo, zookeeper]
			@Override
			public void process(WatchedEvent event) {
				System.out.println("========================");
			}
		});
		
		System.out.println(nodes);
		
		Thread.sleep(Long.MAX_VALUE);
	}
	
	
	/**
	 * 删除zNode
	 */
	@Test
	public void deleteZnode() throws Exception {
		
		//不知道具体version就写-1
		zkClient.delete("/alibaba",-1);
	}
	
	
	/**
	 * 创建zNode
	 */
	
	@Test
	public void createZnode() throws Exception {
		
				//1. 在哪个节点创建
				//2.存的数据
				//3. acl: 权限，开放不安全（基本都用跟这个）
				//4. 是持久还是临时，persistent
		//返回一个节点位置
		String path = 
				zkClient.create("/alibaba",  "test".getBytes() , Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println("path: " + path );
	}
	
	
	/**
	 * 获取Zookeeper 客户端对象
	 */
	@Before
	public void init() throws Exception {
		
		//通过new可以获取zookeeper的对象
		//connectString: zookeeper的集群信息（字符串的形式），中间用逗号隔开
		//sessionTimeout: 单位毫秒，最小/最大超时时间，给的值要在范围内，然后看离哪个值更近
		//fixme: watcher：监听器（默认的）：当发生改变时，输出里面的内容
		
		//事件触发
		
	 	zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
					@Override
					public void process(WatchedEvent event) {
						System.out.println("Zookeeper Client  init Success ......");
					}
				});
//	 	System.err.println("zkClient: " + zkClient);
	}
	
	//@After
	public void close() throws Exception {
		zkClient.close();
	}
}
