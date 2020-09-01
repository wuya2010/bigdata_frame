package com.alibaba.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class HBaseUtil {
	
	
	
/*	1. 连接的接口： connection里面又与master和zookeeper，regions的连接
	2.怎么获取： 通过connetcionfactory进行连接
	3.通过table与admin对象进行连接
	4.对数据进行增删改，需要获取table： DDL
	5.对表的操作 ，需要用admin获取连接: 
	6. connection做一个单例，重量级安全的
	7. table与admin轻量级的，并且是不安全的，不能在线程中共享，不推荐缓存或池化，用完之后就关闭
	*/
	private static Connection connection = null;
	
	
	//构建对象
	static {
		 try {
			 //工具类： HBaseconfiguration
			 Configuration conf = HBaseConfiguration.create();
			 //这里只放一个zookeeper的连接，就可以获取其他的连接
		     conf.set("hbase.zookeeper.quorum", "hadoop105,hadoop106,hadoop107");
	         conf.set("hbase.zookeeper.property.clientPort", "2181");
			 connection = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	
	
	
	   /**
     * 创建表
     *
     * @param tableName
     * @param families
     * @throws IOException
     *                ==========》传入的参数： 表名  + 列族  String... 可变参数
     */
	public static void createTable(String tableName, String... families) throws Exception {
		
		//获得admin对象，建立与master的连接  ==>调用getAdmin()就可以获取这个对象
		Admin admin = connection.getAdmin();
		
		//判断语句
		if (admin.tableExists(TableName.valueOf(tableName))) {
			System.out.println("table " + tableName + " already exists");
			//直接return
			admin.close();

			return;
		}
		
		
		//表的描述器中加入表名
		HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));

		//列族  ，有可能有多个，所以循环调用
		for (String family : families) {
			
			//列的描述器
			HColumnDescriptor familyDesc = new HColumnDescriptor(family);
			//在描述器中加入列名
			tableDesc.addFamily(familyDesc);
		}
		
		//表的描述器
		admin.createTable(tableDesc);
		
		//一定要记得关
		admin.close();	
	}
	
	
	
	
	
	  /**
     * 修改表中的某个列族
     *
     * @param tableName
     * @param family
     * @throws IOException
     */
	public static void modifyTable(String tableName, String family) throws Exception {
		
		//获得admin对象
		Admin admin = connection.getAdmin();
		
		//列族的描述器
		HColumnDescriptor familyDesc = new HColumnDescriptor(family);
		
		//version == > 默认是1
		familyDesc.setMaxVersions(3);

		//modifycolum： 修改列族   column指的列族         ===》modifyTable比较麻烦
		admin.modifyColumn(TableName.valueOf(tableName), familyDesc);

		admin.close();
	}
	
	
	
    /**
     * 删除表
     *
     * @param tableName
     * @throws IOException
     */
	public static void dropTable(String tableName) throws Exception {
		
		Admin admin = connection.getAdmin();
		
		if (!admin.tableExists(TableName.valueOf(tableName))) {
			System.err.println("table " + tableName + " doesn't exist");
			return;
		}
		
		//这里也要分两步
		admin.disableTable(TableName.valueOf(tableName));
		admin.deleteTable(TableName.valueOf(tableName));

		admin.close();
}
	
	
	
    /**
     * 插入一个列的值
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @param value
     * @throws IOException
     */
	public static void putCell(String tableName, String rowKey, 
			String family, String column, String value) throws Exception {
		
		//获取一个table对象
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		//row放进去  把字符串转换成byte==》根据rowkey获取是往哪一行插入数据
		Put put = new Put(Bytes.toBytes(rowKey));
		
		//==》 addColumn(family, qualifier, this.ts, value); 
		// 这里放一个put的描述器   这里指的是列，而不是列族
		put.addColumn((Bytes.toBytes(family)), Bytes.toBytes(column),
				Bytes.toBytes(value));
		
		//系统方法： putCell(tableName, rowKey, family, column, value);
		
		//put方法==》 put(Put put)放一个put对象==》 Put(byte [] row)==》rowkey 
		//没有返回值
		table.put(put);
		
		table.close();
	}
	
	
	
	  /**
     * 获取一行数据===》     单行  get 和scan方法
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
	public static void getRow(String tableName, String rowKey) throws Exception {
		
		
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		
		Get get = new Get(Bytes.toBytes(rowKey));
		
		//返回的是一个列==》get(Get get)===》Get(byte [] row) 
		// public Result get(final Get get) throws IOException===》有返回值
		//返回的是一行的结果，而scan是多行的
		Result result = table.get(get);
	
		
		
		//一行可能有多个列
		//获取列的信息==>数组
		Cell[] cells = result.rawCells();

		//每一个cell都是k-v
		for (Cell cell : cells) {
			
			//提供了这个方法获取 工具类==>得到value
			byte[] valueBytes = CellUtil.cloneValue((cell));
			//获取列名信息
			byte[] columnBytes = CellUtil.cloneQualifier(cell);
			System.out.println(Bytes.toString(columnBytes) + "-" + Bytes.toString(valueBytes));
		}
		
		table.close();
	
	}
	
	
	
	/**
     * 根据rowKey范围获取====》多行数据
    *
    * @param tableName
    * @param startRow
    * @param stopRow
    * @throws IOException
    */
	
	public static void getRowsByRowRange(String tableName, String startRow,
		String stopRow) throws Exception {
		
		Table table = connection.getTable(TableName.valueOf(tableName));

		//===》Scan(byte [] startRow, byte [] stopRow)
		Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));

		//===》ResultScanner(返回值) getScanner(final Scan scan) 
		ResultScanner scanner = table.getScanner(scan);

		//遍历所有行数据
		for (Result result : scanner) {
		
			//获取一列的信息==>导包
			Cell[] cells = result.rawCells();

			//每一个cell都是k-v
			for (Cell cell : cells) {
				//提供了这个方法获取 工具类
				byte[] valueBytes = CellUtil.cloneValue((cell));
				//获取列信息
				byte[] columnBytes = CellUtil.cloneQualifier(cell);
				System.out.println(Bytes.toString(columnBytes) + "-" + Bytes.toString(valueBytes));
			}
		}
	
		//建立的连接，数据是点一点的传的,所以也需要关闭
		scanner.close();
		table.close();
	
	}
	
	
	

	
	
    /**
     * 根据过滤器获取===》通过过滤的条件来获取 ， 多行数据
     *
     * @param tableName
     * @param family
     * @param column
     * @param value
     * @throws IOException
     */

	public static void getRowsByColumn(String tableName, String family, String column, String value) throws Exception {
		
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		Scan scan = new Scan();
		
		//SingleColumnValueFilter： 列的值的过滤器，以具体某个值为过滤条件
		//底层都以rowkey进行扫描，全表扫描，性能比较低
		//写入过滤条件的字段==》列族 + 列名 + 比较
		SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(column),
				CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value));

		
		//如果过滤没有哪个条件，比如name字段没有，那么就不显示==》这个要注意了
		filter.setFilterIfMissing(true);
		//传入过滤器
		scan.setFilter(filter);

		//返回scanner
		ResultScanner scanner = table.getScanner(scan);
		
		
		//遍历scanner
		for (Result result : scanner) {
			
			Cell[] cells = result.rawCells();
			
			for (Cell cell : cells) {
				
				 byte[] rowBytes = CellUtil.cloneRow(cell);
	                byte[] valueBytes = CellUtil.cloneValue(cell);
	                byte[] columnBytes = CellUtil.cloneQualifier(cell);
	                System.out.println(Bytes.toString(rowBytes) + "-" 
	                + Bytes.toString(columnBytes) + "-" + Bytes.toString(valueBytes));
				
			}	
	
		}
		

        scanner.close();
        table.close();
        
        //  this.prefixes = new String[] {prefix};
        //new PrefixFileFilter()
	
	}
	
	
	

	
	
	
	  /**
     * 根据 ===》多个过滤器获取多行数据
     *
     * @param tableName
     * @param family
     * @param column
     * @param value
     * @throws IOException
     */

	  //todo : 多个条件进行过滤
	public static void getRowsByColumn2(String tableName, String family, String column, String value) throws Exception {
		
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		Scan scan = new Scan();

		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(column),
				CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value));
			
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(column),
				CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value));
		
		filter1.setFilterIfMissing(true);
		filter2.setFilterIfMissing(true);
		
		//新建一个filter的集合
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		filterList.addFilter(filter1);
		filterList.addFilter(filter2);
		
		
		//返回scanner
		ResultScanner scanner = table.getScanner(scan);
		
		//遍历scanner
		for (Result result : scanner) {
			
			Cell[] cells = result.rawCells();
			
			for (Cell cell : cells) {
				
				 byte[] rowBytes = CellUtil.cloneRow(cell);
	                byte[] valueBytes = CellUtil.cloneValue(cell);
	                byte[] columnBytes = CellUtil.cloneQualifier(cell);
	                System.out.println(Bytes.toString(rowBytes) + "-" 
	                + Bytes.toString(columnBytes) + "-" + Bytes.toString(valueBytes));
				
			}
			
		

		}
        scanner.close();
        table.close();
	}
	
	
    /**
     * 删除一行数据
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void deleteRow(String tableName, String rowKey) throws IOException {
    	
    	Table table = connection.getTable(TableName.valueOf(tableName));

    	//新建一个delete，放入rowkey
    	Delete delete = new Delete(Bytes.toBytes(rowKey));

    	table.delete(delete);

        table.close();
    }


    /**
     * 删除指定列的数据===>指定的列
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @throws IOException
     */
	//delete.addColumn(family, qualifier)==》删除列下面的最新版本
	//delete.addColumns(family, qualifier)==》删除列下面的所有版本
	
    public static void deleteColumn(String tableName, String rowKey, String family, String column) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));

        Delete delete = new Delete(Bytes.toBytes(rowKey));

        //删除列下面的最新版本
        delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));

        table.delete(delete);

        table.close();
    }
    
    
    
	
	
	public static void main(String[] args) throws Exception {
		//dropTable("class");
		
		//createTable("person", "info");
		//modifyTable("class","info");
	   //putCell("person", "1002", "info", "name", "wangwu");  //String tableName, String rowKey, String family, String column, String value
	
		//getRow("person",  "1001") ;
		//getRowsByRowRange("person", "1001","1002");
		
		//getRowsByColumn("person", "info", "name", "wangwu");
		
		//deleteRow("person","1001");
		
		 deleteColumn("person", "1002", "info", "name") ;
	}
	
}
