package com.alibaba.interceptor2;

import org.apache.commons.lang.math.NumberUtils;

public class LogUtils {


/*
 * 对数据进行验证
 * 
 */
	//validate： 证实
	public static boolean validateStart(String log) {
		  
			// 如果为空，返回false
			if (log == null){
	            return false;
	        }

	        // 校验json ： 如果不是{ }开始结果的
	        if (!log.trim().startsWith("{") || !log.trim().endsWith("}")){
	            return false;
	        }

	        return true;

	}
	
	

	public static boolean validateEvent(String log) {
        // 1549696569054 | {"cm":{"ln":"-89.2","sv":"V2.0.4","os":"8.2.0","g":"
		//M67B4QYU@gmail.com","nw":"4G","l":"en","vc":"18","hw":"1080*1920","ar
		//":"MX","uid":"u8678","t":"1549679122062","la":"-27.4","md":"sumsung-12","vn
		//":"1.1.3","ba":"Sumsung","sr":"Y"},"ap":"weather","et":[]}	
		
		// ==》说明    文件格式：  服务器时间 | json	
		//数据切割
		String[] logContents  = log.split("\\|");	
		
		//验证长度
		if (logContents.length!=3) {
			return false;
		}
		
		//验证时间戳==>!NumberUtils.isDigits(logContents[0] 不是数字    ==》数字长度  !=13 或是
		//基本类型，基本数据一定要清洗
		if (logContents[0].length()!=13|| !NumberUtils.isDigits(logContents[0])) {
			return false;
		}

		//验证json格式    ==>	trim()
		
		if (!logContents[1].trim().startsWith("{") || !logContents[1].trim().endsWith("}")) {
			return false;
		}

		return true;
	}

}
