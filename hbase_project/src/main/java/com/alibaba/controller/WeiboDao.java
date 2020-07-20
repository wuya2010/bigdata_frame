package com.alibaba.controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//DAO(Data Access Object)  ： 数据库接口
/*
 * 建立各个方法的连接
 * create
 * put
 * get
 * delete
 *
*/

//这个是与HBase进行交互

public class WeiboDao {

	public static Connection connection = null;
	
	
	//初始化代码块
	static {
		
		try {
			Configuration conf = HBaseConfiguration.create();
			
			conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop103");
			
			connection = ConnectionFactory.createConnection(conf);
		}catch (Exception e) {
			e.printStackTrace();
		}	
	}
	
	
	/*
	 * 创建namespace
	 * 
	 * 
	 */
	public void createNamespace(String namespaceWeibo) throws Exception {
		
		Admin admin = connection.getAdmin();
		
		NamespaceDescriptor namespaceDesc;
		
		try {
			
			 admin.getNamespaceDescriptor(namespaceWeibo);
			 
		} catch (Exception e) {
			
			// 返回值：public static Builder create(String name)
			//build方法： public NamespaceDescriptor build() 
			namespaceDesc = NamespaceDescriptor.create(namespaceWeibo).build();
			admin.createNamespace(namespaceDesc);
			
		}finally {
			
			admin.close();
		}
	}
	
	
	/*
	 * 创建表方法
	 * 
	 */
	public void createTable(String tableName,Integer versions,String... families) throws Exception {

		Admin admin = connection.getAdmin();
		
		//判读
		if (admin.tableExists(TableName.valueOf(tableName))) {
			System.out.println("this table is already exit");
		}
		
		HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
		
		for (String family : families) {
			
			HColumnDescriptor familydesc = new HColumnDescriptor(family);
			
			familydesc.setMaxVersions(versions);
			tableDesc.addFamily(familydesc);
			
		}
		
		admin.createTable(tableDesc);
		
		admin.close();
		
	}

	
	
	/*
	 * 
	 * 方法的重载: 调用上面的方法
	 * 
	 * 
	 */
	public void createTable(String tableName, String family) throws Exception {

		int versions = 1;
		
		createTable(tableName, versions, family);

	}
	


	
	
	/*
	 * 
	 * 通过前缀获取内容
	 * 
	 */
	public List<String> getRowKeysByPrefix(String tableName, String prefix) throws Exception {
		
		List<String> rowkeys = new ArrayList<>();
		
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		Scan scan = new Scan();
		
		scan.setRowPrefixFilter(Bytes.toBytes(prefix));
		
		ResultScanner scanner = table.getScanner(scan);
		
		for (Result result : scanner) {
			
			//===》获取
			byte[] row = result.getRow();
			//创建一个集合放进去
			//Bytes.toString也是转换
			rowkeys.add(new String(row));
		}

		scanner.close();
		table.close();
		
		return rowkeys;
		
	}

	
	
	/*
	 * 插入一行数据
	 */
	public void putCell(String tableName, String family,
			String Rowkey, String column, String value) throws Exception {
		
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		Put put = new Put(Bytes.toBytes(Rowkey));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
		
		table.put(put);
		
		table.close();
		
	}
	
	
	
	public void putcells(String tableName, List<String> rowkeys, String family, String column,
			String value) throws Exception {
		// TODO Auto-generated method stub
		
		if (rowkeys.size() ==0) {
			return;
		}
		
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		List<Put> puts = new ArrayList<>();
		
		for (String rowkey : rowkeys) {
			
			Put put = new Put(Bytes.toBytes(rowkey));
			
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
			
			puts.add(put);
		}
		table.put(puts);
		
		table.close();
		
		
	}

	
	/*
	 * 获取row范围的数据
	 * 
	 */
	public List<String> getRowKeysByRangs(String tableName, String startRow, String stopRow) throws IOException {

		List<String> rowkeys = new ArrayList<>();
		
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		Scan scan = new Scan(Bytes.toBytes(startRow),Bytes.toBytes(stopRow));
		
		ResultScanner scanner = table.getScanner(scan);
		
		for (Result result : scanner) {
			
			
			//getRow()方法获得的什么
			byte[] row = result.getRow();
			
			rowkeys.add(Bytes.toString(row));
			
		}
		
		scanner.close();
		table.close();
		
		return rowkeys;
	}

	
	/*
	 * 
	 * 获取get对应的value
	 */
	public List<String> getRow(String tableName, String rowkey) throws Exception {
		
		
		List<String> list = new ArrayList<>();
		
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		Get get = new Get(Bytes.toBytes(rowkey));
		
		Result result = table.get(get);
		
		Cell[] cells = result.rawCells();
		
		for (Cell cell : cells) {
			
			list.add(Bytes.toString(CellUtil.cloneValue(cell)));
			
		}

		table.close();
		
		return list;
	}

	
	
	/*
	 * 
	 *  setRowPrefixFilter(Bytes.toBytes(prefix));
	 * 
	 */
	public List<String> getCellByPrefix(String tableName, String prefix, String family, String column) throws Exception {
		
		List<String> list = new ArrayList<>();
		
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		Scan scan = new Scan();
		
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
		scan.setRowPrefixFilter(Bytes.toBytes(prefix));
		
		
		ResultScanner scanner = table.getScanner(scan);
		
		for (Result result : scanner) {
			
			Cell[] cells = result.rawCells();

			//????
			list.add(Bytes.toString(CellUtil.cloneValue(cells[0])));
			
		}
		
		table.close();
		
		return list;
		
	}

	/*
	 * 
	 * 
	 */
	public List<String> getCellsByRowkeys(String tableName, String family, List<String> rowKeys,String column) throws Exception {
		
			
			Table table = connection.getTable(TableName.valueOf(tableName));
			
			  List<Get> gets = new ArrayList<>();
		      List<String> weibos = new ArrayList<>();
			
			
		      for (String rowkey : rowKeys) {
				
		    	  Get get = new Get(Bytes.toBytes(rowkey));
		    	    get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
		            gets.add(get);
			}
		      
		      Result[] results = table.get(gets);
		      
		      
		      for (Result result : results) {
		            Cell[] cells = result.rawCells();
		            weibos.add(Bytes.toString(CellUtil.cloneValue(cells[0])));
		        }
		        return weibos;
			
	}






	public void deleteColumn(String tableName, String rowkey, String family, String column) throws IOException {
		// TODO Auto-generated method stub
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		Delete delete = new Delete(Bytes.toBytes(rowkey));
        delete.addColumns(Bytes.toBytes(family), Bytes.toBytes(column));
		
		table.delete(delete);
		table.close();
		
		
	}


	public void deleteRow(String tableName, String rowkey) throws Exception {
		// TODO Auto-generated method stub
		
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		Delete delete = new Delete(Bytes.toBytes(rowkey));
		
		table.delete(delete);
		table.close();
		
	}


	


	
	
	
	
	
}
