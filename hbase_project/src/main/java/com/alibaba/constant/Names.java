package com.alibaba.constant;

//在这里类中将所有需要用的常量都写在里面  ； 所有需要的常量

public class Names {

	//定义一个namespace
	public static final  String NAMESPACE_WEIBO = "weibo";
	
	//需要建立的表
	public static final String TABLENAME_WEIBO = "weibo:weibo";
	public static final String TABLENAME_RELATION = "weibo:relation";
	public static final String TABLENAME_INDEX = "weibo:index";
	
	
	//表中的常量==》列族
	public static final String WEIBO_FAMILY_DATA = "data";
	public static final String RELATION_FAMILY_DATA = "data";
	public static final String INDEX_FAMILY_DATA = "data";

	
	public static final String WEIBO_COLUMN = "content";
	//建立关系的时间
	public static final String RELATION_COLUMN = "time";
	
	
	public static final int INDEX_VERSIONS = 3;
	
}
