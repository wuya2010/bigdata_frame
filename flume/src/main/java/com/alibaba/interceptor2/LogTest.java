package com.alibaba.interceptor2;

public class LogTest {

		public static void main(String[] args) {
			
			LogUtils test = new LogUtils();
			
			String log = "1566563097472|{\"cm\":{\"ln\":\"-58.8\",\"sv\":\"V2.6.4\",\"os\":\"8.2.4\",\"g\":\"8GQ7R5N2@gmail.com\",\"mid\":\"0\",\"nw\":\"4G\",\"l\":\"pt\",\"vc\":\"8\",\"hw\":\"640*1136\",\"ar\":\"MX\",\"uid\":\"0\",\"t\":\"1566542979959\",\"la\":\"22.5\",\"md\":\"Huawei-3\",\"vn\":\"1.0.4\",\"ba\":\"Huawei\",\"sr\":\"F\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1566467054482\",\"en\":\"display\",\"kv\":{\"goodsid\":\"0\",\"action\":\"1\",\"extend1\":\"1\",\"place\":\"2\",\"category\":\"20\"}}";
			
			System.out.println(test.validateEvent(log));
			
			
			
			
		}
	
}
