package com.alibaba.service;

import com.alibaba.constant.Names;
import com.alibaba.controller.WeiboDao;
import java.util.ArrayList;
import java.util.List;

/*
 * 1. 初始化     init()
 * 2.发微博        publish()
 * 3.关注默认    follow()
 * 4.取消关注     unfollow()
 * 5.通过rowkey前缀：  getCellsByPrefix
 * 6.获取最新的微博：  getAllRecentWeibos
 * 
 */


// 第二部: 写具体的所有的具体逻辑 ， 然后在这里写的东西需要什么就去找什么 ==》 dao层  ==》常量层


public class WeiboService {
	
	
	//这里需要频繁调用dao里面连接数据库的方法，所以需要新建
	WeiboDao dao = new WeiboDao();
	
	//1.  初始化==>新建三个表
	/*
	 * 初始化，分别根据关系建立三个表
	 * 
	 * 
	 */
	public void init() throws Exception {
		
		
		
		//首先建一个namespace	
		dao.createNamespace(Names.NAMESPACE_WEIBO);
		//创建微博表
		dao.createTable(Names.TABLENAME_WEIBO,Names.WEIBO_FAMILY_DATA);
		//3) 创建用户关系表
		dao.createTable(Names.TABLENAME_RELATION,Names.RELATION_FAMILY_DATA);
	     //4) 创建用户微博内容接收邮件表
		dao.createTable(Names.TABLENAME_INDEX,Names.INDEX_VERSIONS,Names.INDEX_FAMILY_DATA);
		
	}

	
	
	
	//1. 发微博 
	//需要新建表，并传入数据==>2个参数： rowkey   ， value
	public void publish(String star, String context) throws Exception {

		//1. 放入数据star
		String Rowkey = star +"_"+System.currentTimeMillis();
		
		dao.putCell(Names.TABLENAME_WEIBO,Names.WEIBO_FAMILY_DATA,Rowkey, 
				Names.WEIBO_COLUMN,context);
		
		
		
		
		
		//后面的逻辑不能掉了==>与其他两个表的关联
		//2. 在relation表      中查找star的所有fansID
		
		String prifix = star + ":Follow:";
		//通过前缀进行查找
		List<String> list = dao.getRowKeysByPrefix(Names.TABLENAME_RELATION,prifix);
		
		//对list进行判断
		if(list.size()<=0) return ;
		
		List<String> fansIds = new ArrayList<>();
		
		//获取所有的fanid
		for (String row : list) {
			String[] split = row.split(":");
			//start : follow  :  fans 
			fansIds.add(split[2]);
		}
		
		
		//3. 将weiboID插入到所有fans的inbox中==>在index表中，rowkey ： fandids
		//表中结构：  key: fansids, value:  Rowkey   ===》让所有fans查看star最新跟新的消息，给的是id而不是直接的数据
		//将weibo表中的rowkey传给value
 		dao.putcells(Names.TABLENAME_INDEX,fansIds,Names.INDEX_FAMILY_DATA,star,Rowkey);
	}
	
	
	
	
	
	
	//2. 关注  ===》2个关注
	//在relation表中添加 2个映射关系的数据，   以star 与fans 与rowkey
	public void follow(String star, String fans) throws Exception {
		
		//1. 在relation表中添加关系
		String Rowkey1 = star + ":Follow:" +fans;
		String Rowkey2 = fans + ":BeFollowedBY:" + star;
		
		dao.putCell(Names.TABLENAME_RELATION, Names.INDEX_FAMILY_DATA, 
				Rowkey1, Names.RELATION_COLUMN, System.currentTimeMillis()+"");
		
		dao.putCell(Names.TABLENAME_RELATION, Names.INDEX_FAMILY_DATA, 
				Rowkey2, Names.RELATION_COLUMN, System.currentTimeMillis()+"");
		
		
		//2.将fans的关系添加到index表中==>从weibo表中获取star的最后三条的weiboID
		//既然是关注的关系，那么就可以获取信息了===》通过方法获得row对应的rowkey
		//2.1 先获取所有的数据
		
		String startRow = star + "_!";
		String stopRow = star + "_|";
		List<String> list = dao.getRowKeysByRangs(Names.TABLENAME_WEIBO,startRow,stopRow);

		//2.2 再取最后的三条==》获取三条信息
		//2.2.1 判断
		if (list.size()<=0) return;
		
		int fromIndex = list.size()>=3?(list.size()-Names.INDEX_VERSIONS):0;
		//将结果放在list中, 得到最后的三行(==》star+ 时间戳)
		List<String> recentWeiboIds = list.subList(fromIndex, list.size());
	
		
		//3.将star的最后三条的weiboId插入到fans的inbox
		for (String recentWeiboId : recentWeiboIds) {
			//插入三条数据
			dao.putCell(Names.TABLENAME_INDEX, Names.INDEX_FAMILY_DATA, fans, star, recentWeiboId);
		}
		
		
	}
	

	//3. 取关
	//删除表中的数据
	public void unfollow(String star, String fans) throws Exception {
		
		String Rowkey1 = star + ":Follow:" +fans;
		String Rowkey2 = fans + ":BeFollowedBY:" + star;
		
		//删除整行
		dao.deleteRow(Names.TABLENAME_RELATION,Rowkey1);
		dao.deleteRow(Names.TABLENAME_RELATION,Rowkey2);
		
		//2. 删除inbox中fans行的star列==》rowkey, family , column 
		dao.deleteColumn(Names.TABLENAME_INDEX,fans,Names.INDEX_FAMILY_DATA,star);
	}

	
	//4. 获取某个star的所有weibo
	public List<String> getCellByPrefix(String tableName, String prefix, String family, String column) throws Exception {
		
		return dao.getCellByPrefix(tableName,prefix,family,column);
		
	}



	//获取近期微博的数量
	public List<String> getAllRecentWeibos(String fans) throws Exception {
		
		 //1.从inbox表中获取fans的所有star的近期weiboId==>value的值
		List<String> list = dao.getRow(Names.TABLENAME_INDEX,fans);
		if (list.size()<=0) {
			return new ArrayList<>();
		}
		
		//2.从weibo表中获取相应的weibo内容
		return dao.getCellsByRowkeys(Names.TABLENAME_WEIBO,Names.WEIBO_FAMILY_DATA,list, Names.WEIBO_COLUMN);
		
		
	}


}
