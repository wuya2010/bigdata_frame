package com.alibaba.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.Arrays;


//todo: 常用api
public class HdfsDemo {
	Configuration conf  = null  ;
	FileSystem fs = null ;
	String uri = "hdfs://hadoop105:9000";
	String user = "etlbat";
	
	/**
	 * 定位读取文件  128M ~ --------------seek
	 */
	@Test
	public void testReadSeek2() throws Exception {
		//输入流
		FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
		
		//输出流
		FileOutputStream fos = new FileOutputStream(new File("d:/hadoopsrc/hadoop-2.7.2.tar.gz.part2"));
		
		//定位到要读取的位置  todo: seek 定位读取
		fis.seek(1024 * 1024 * 128 );
		
		//流的对拷
		IOUtils.copyBytes(fis, fos, conf);
	
		
		//关闭资源
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
	}
	
	
	
	/**
	 * 定位读取文件   0~128M
	 */
	@Test
	public void testReadSeek1() throws Exception {
		//输入流
		FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
		
		//输出流
		FileOutputStream fos = new FileOutputStream(new File("d:/hadoopsrc/hadoop-2.7.2.tar.gz.part1"));
		
		//流的对拷
		byte [] buf = new byte[1024];
		for(int i = 0 ; i < 1024 *128 ;  i ++) {
			fis.read(buf);
			fos.write(buf);
		}
		
		//关闭资源
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
	}
	
	
	
	/**
	 * 测试七: 基于IO流下载文件（从HDFS下载到本地）
	 */
	@Test
	public void testDownloadFileOnIO() throws Exception {
		//1. 输入流  todo： 写出 fs.open
		FSDataInputStream fis = fs.open(new Path("/0508/dashenban/banzhang.txt"));
		
		//2. 输出流
		FileOutputStream fos = new FileOutputStream(new File("d:"+File.separator +"hadoop"+File.separator+"banzhang.txt"));
		
		//3. 流的对拷
		
		IOUtils.copyBytes(fis, fos, conf);
		
		//4. 关闭资源
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		
	}
	
	/**
	 * 测试六: 基于IO流上传文件:FSDataoutputstream... IOutils.copybytes（从本地上传到HDFS）
	 */
	@Test
	public void testUploadFileOnIO() throws Exception {
		//1. 输入流  读取本地的文件
		FileInputStream fis = new FileInputStream(new File("d:/hadoop/ni.txt"));
		
		//2. 输出流 将数据写入到HDFS todo： 数据写入 fs.create
		FSDataOutputStream fos = fs.create(new Path("/0508/dashenban/xiaoguoguo.txt"));

        //3. todo : 流的对拷  copyBytes
		IOUtils.copyBytes(fis, fos, conf);
		
		
		//4. 关闭资源
		
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		
		
	}
	
	
	/**
	 * 测试五: 文件夹或者文件的判断
	 */
	@Test
	public void testListStatus() throws Exception {
		
		
		
//		FileStatus[] listStatus = fs.listStatus(new Path("/"));
//		for (FileStatus fileStatus : listStatus) {
//			if(fileStatus.isFile()) {
//				System.out.println("file:" + fileStatus.getPath().getName());
//			}else {
//				System.out.println("dir:" + fileStatus.getPath().getName());
//			}
//		}
		
		printFileOrDir("/", fs);
		
	}
	
	/**
	 * 传入一个路径 ，递归将该路径下的所有的文件还有目录打印到控制台
	 */
	public void printFileOrDir(String path , FileSystem fs) throws Exception {
		
		FileStatus[] listStatus = fs.listStatus(new Path(path));
		
		for (FileStatus fileStatus : listStatus) {
			//判断是文件还是目录
			if(fileStatus.isFile()) {  //todo: 文件或是目录
				System.out.println("File:" + path + "/" + fileStatus.getPath().getName());
			}else {
				// path:   hdfs://hadoo1p102:9000/0508
				System.out.println(fileStatus.getPath());
				
				System.out.println("---------------------------------");
				//fixme: substring 截取
				String currentPath = fileStatus.getPath().toString().substring("hdfs://hadoop102:9000".length());

				//打印当前的目录
				System.out.println("Dir:" + currentPath);
				
				//继续迭代当前目录下的子目录及文件
				printFileOrDir(currentPath, fs);
			}
			
		}
		
	}
	
	
	
	
	
	
	
	/**
	 * 测试四: 文件详情查看
	 */
	@Test
	public void testListFiles() throws Exception{
		RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path("/"), true);

		while(remoteIterator.hasNext()) {
			//获取下一个
			LocatedFileStatus fileStatus = remoteIterator.next();
			//获取文件具体的信息
			System.out.println(fileStatus.getPath().getName());
			//hdfs://hadoop102:9000/user/atguigu/liubei.txt
			System.out.println(fileStatus.getReplication());
			System.out.println(fileStatus.getPermission());
			System.out.println(fileStatus.getBlockSize());//getBolockSize getBlockSize
			System.out.println(fileStatus.getLen());
			BlockLocation[] blockLocations = fileStatus.getBlockLocations();
			
			System.out.println(blockLocations);
			//[Lorg.apache.hadoop.fs.BlockLocation;@662706a7
			
			System.out.println("------------------------------");
			
			
			for (BlockLocation blockLocation : blockLocations) {
				System.out.println(blockLocation);
				//0,15429,hadoop102,hadoop103
				System.out.println("》》》》》》》》》》》》》》》》》》");
				String[] hosts = blockLocation.getHosts();

				System.out.println(Arrays.toString(hosts));
			}
			
			System.out.println("=====================================");
		}
	}
	
	/**
	 * 测试三: 文件更名
	 */
	@Test
	public void testRename()  throws Exception{
		
		fs.rename(new Path("/0508/dashenban/longlong.txt"), new Path("/0508/dashenban/xiaolonglong.txt"));
	}
	
	
	/**
	 * 测试三: 文件夹删除
	 */
	@Test
	public void testDelete() throws Exception {
		
		fs.delete(new Path("/user"), true);
	}
	
	/**
	 * 测试二: 从HDFS下载文件
	 */
	@Test
	public void testCopyToLocal() throws Exception {
		
		fs.copyToLocalFile(new Path("/NOTICE.txt"),new Path("d:/hadoopsrc/NOTICE.txt"));
		//fs.copyToLocalFile(false,new Path("/NOTICE.txt"),new Path("d:/hadoopsrc/NOTICE.txt"), true);
	}


	//todo: test before
	@Before
	public void before() throws Exception {
		conf = new Configuration();
		fs  = FileSystem.get(new URI(uri), conf,user);
	}


    //todo: test after
//	@After
//	public void after()  throws Exception{
//		fs.close();
//	}
	
	
	
	/**
	 * 测试一: 上传文件到HDFS
	 */
	@Test
	public void testCopyFromLocal() throws Exception {
		//1. 获取FileSystem对象
		Configuration conf = new Configuration();
		conf.set("dfs.replication", "2");
		
		FileSystem fs  = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "atguigu");
		//2. 操作  (Path src  本地 , Path dst  --to hdfs)
		fs.copyFromLocalFile(new Path("d:/hadoopsrc/longlong.txt"), new Path("/0508/dashenban"));
		//3. 关闭资源
		fs.close();
	}
	
	/**
	 * 打通客户端与HDFS的连接
	 */
	@Test
	public void testClientConnectHDFS() throws Exception {
		//Configuration
		Configuration conf  = new Configuration();


		//fixme: -- FileSystem: 获取对应的system
		FileSystem fs  = FileSystem.get(new URI("hdfs://hadoop105:9000"), conf, "etlbat");
		
		//在HDFS上创建一个目录  /0508/dashenban
		fs.mkdirs(new Path("/test_hdfs"));
		
		//关闭资源
		fs.close();
		
	}
	
	
	
	
}
