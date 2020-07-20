package com.alibaba.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;



//A Generic User-defined Table Generating Function (UDTF)

public class ParseEventsJson2  extends GenericUDTF{

	/*
	 * initialize: 初始化，返回当前函数的元数据信息
	 * process: 真正的数据处理
	 * 			传入一条数据，写出多行
	 * 			如果写出的一行又多列，通常创建一个数组，
	 * close: 处理完后执行清理工作
	 */
	
	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
		
		//函数返回一行中的列名
		List<String> structFieldNames =new ArrayList<>();
		structFieldNames.add("event_name");
		structFieldNames.add("event_json");
		
		//函数返回的一行中列的类型
		List<ObjectInspector> structFieldObjectInspectors = new ArrayList<>();
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		
		
		return ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
	}
	
	//[{},{},{}]  真正的逻辑在这里
	@Override
	public void process(Object[] args) throws HiveException {
		// TODO Auto-generated method stub
	
		String data = args[0].toString();
		
		//isBlank: 不能是whitedata
		//isEmpty:
		if (StringUtils.isBlank(data)) {
			return;
		}
		
		//构建jsonArray
			
			//每个jsonArray中一个{},输出一次
		try {
			JSONArray jsonArray = new JSONArray(data);
			
			for(int i = 0 ; i< jsonArray.length();i++) {
				
			try {
				String[] result =  new String[2];
				
				//event_name
				result[0] = jsonArray.getJSONObject(i).getString("en");
				//event_json
				result[1] = jsonArray.getJSONObject(i).toString();
				
				forward(result);
				
			}catch(Exception e) {
				continue;
			}
			}
		}catch(JSONException e) {
			e.printStackTrace();
		}
	
	}

	
	
	
	
	
	@Override
	public void close() throws HiveException {
		// TODO Auto-generated method stub
		
	}

		
	
	
}
