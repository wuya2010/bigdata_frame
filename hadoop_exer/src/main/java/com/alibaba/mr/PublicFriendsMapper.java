package com.alibaba.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/*
以下是博客的好友列表数据，冒号前是一个用户，冒号后是该用户的所有好友？
求出哪些人两两之间有共同好友，及他俩的共同好友都有谁？
输入文件：
A:B,C,D,F,E,O
B:A,C,E,K
C:F,A,D,I
D:A,E,F,L
E:B,C,D,M,L
F:A,B,C,D,E,O,M
G:A,C,D,E,F
H:A,C,D,E,O
I:A,O
J:B,O
K:A,C,D
L:D,E,F
M:E,F,G
O:A,H,I,J

思路：
1. 读取数据， maptask 将数据转换成 key-> value；获取关注A的人；
2. 同时包含了 A + B 的 则为公共的好友
 */
public class PublicFriendsMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	
	TreeMap<String,String> map = new TreeMap<>();
	
	Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// 将每一行转换成key，value的形式放进map中
		String line = value.toString();
		String[] split = line.split(":");
		map.put(split[0], split[1]);
		
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		
		/*
		 * 思路：将A的共同好友与B,C等其他好友进行比较，将B的共同好友与C,D等其他好友进行比较，......依次类推
		 * 需要3层循环嵌套
		 */
		
		// 获取map中所有的key
		Set<String> keySet = map.keySet();
		// 将key放进数组中，方便遍历
		String[] array =  keySet.toArray(new String[keySet.size()]);

		// 从A开始进行循环
		for (int i = 0; i < array.length; i++) {
			String outerValues = map.get(array[i]);//获取A的value，方便后面进行比较  todo:value
			
			// 从A下面一个key，即B开始循环
			for (int j = i + 1; j < array.length; j++) {
				
				// 声明一个临时数组，存放共同好友
				List<String> commonList = new ArrayList<>();
				
				// 获取B的value
				String innerValues = map.get(array[j]);
				// 切开B的value
				String[] split = innerValues.split(",");
				// 将B的好友逐个与A的好友进行对比
				for (int k = 0; k < split.length; k++) {
					if(outerValues.contains(split[k])) {// 如果B中的好友A中也有，则放到公共好友集合中
						commonList.add(split[k]);
					}
				}
				
				if(commonList.size() != 0) {
					// 生成最终k，凑格式
					String str = new String();
					str = array[i] + "-" + array[j] + "\t";
					for (String string : commonList) {
						str += string + "\t";
					}
					
					//输出
					k.set(str);
					context.write(k, NullWritable.get());
				}
				
			}
		}
		
		
		
	}

}
