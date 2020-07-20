package com.alibaba.etl;

public class EtUtils {

	public static String etData(String line) {
		
		StringBuffer sbs = new StringBuffer();
		
		
		//1
		String[] split = line.split("\t");
		
		
		//2
		if(split.length < 9 ) { 
			return null;
		}
		
		//3
		split[3].replace(" ", "");
		
		//4
		for (int i = 0; i < split.length; i++) {
			
			if(i < 9) {
				if (i==split.length-1) {
					sbs.append(split[i]);
				}else {
					sbs.append(split[i]).append("\t");
				}
			}else {
				
				if (i==split.length-1) {
					sbs.append(split[i]);
				}else {
					sbs.append(split[i]).append("&");
				}
				
				
			}
			
			
		}
		
		
		return sbs.toString();	
	}
	
}
