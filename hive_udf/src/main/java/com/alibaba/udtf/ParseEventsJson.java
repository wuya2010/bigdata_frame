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

/*
 * 1.initialize:  初始化。 返回当前函数的元数据信息（返回的数据类型和数据的字段名） 
 * process：   真正处理数据。
 * 				传入一条数据，写出多行。
 * 				如果写出的一行数据有多列，通常是创建一个数组，这个数组中将所有的列字段放入
 * 					forward(xxxx);
 *   close：  最后处理完执行清理操作！
 */

//数据项目中，对数据结构的转换
public class ParseEventsJson extends GenericUDTF{
	
	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
		
		// 函数返回的一行中的列名
		List<String> structFieldNames=new ArrayList<>();
		
		structFieldNames.add("event_name");
		structFieldNames.add("event_json");
		
		// 函数返回的一行中的列的类型
		List<ObjectInspector> structFieldObjectInspectors=new ArrayList<>();
		
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		
		return ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
	}

	// [{},{},{}]
	@Override
	public void process(Object[] args) throws HiveException {
		
		if (args[0]==null) {
			return;
		}
		
		String data = args[0].toString();
		
		// isBlank:   判断不能是"",且不是null，且不能是whileblank字符(空格，回车，\t)
		// isNotEmpty:  判断不能是"",且不是null，返回true
		if (StringUtils.isBlank(data)) {
			return;
		}
		
		// 构建JSONArray
		try {
			JSONArray jsonArray = new JSONArray(data);
			
			// 每一个JsonArray中的每一个{},输出一次
			// 输出的一行，两个字段
			for (int i = 0; i < jsonArray.length(); i++) {
				
				try {
					String [] result=new String[2];
					
					// event_name
					result[0]=jsonArray.getJSONObject(i).getString("en");
					// event_json
					result[1]=jsonArray.getJSONObject(i).toString();
					
					//写出
					forward(result);
				} catch (Exception e) {
					continue;
				}
			}
			
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
		
	}

	@Override
	public void close() throws HiveException {
	}

}