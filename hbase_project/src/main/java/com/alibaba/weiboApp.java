package com.alibaba;

import com.alibaba.controller.Weibocontroller;

/**
 * @author kylinWang
 * @data 2020/7/19 9:26
 *
 * hbase商业应用能力：
 * 每天：
 * 1) 消息量：发送和接收的消息数超过60亿
 * 2) 将近1000亿条数据的读写
 * 3) 高峰期每秒150万左右操作
 * 4) 整体读取数据占有约55%，写入占有45%
 * 5) 超过2PB的数据，涉及冗余共6PB数据
 * 6) 数据每月大概增长300千兆字节
 *
 *
 *
 * 实现：
 * 1) 微博内容的浏览，数据库表设计
 * 2) 用户社交体现：关注用户，取关用户
 * 3) 拉取关注的人的微博内容
 *
 * 步骤：
 * 1) 创建命名空间以及表名的定义
 * 2) 创建微博内容表
 * 3) 创建用户关系表
 * 4) 创建用户微博内容接收邮件表
 * 5) 发布微博内容
 * 6) 添加关注用户
 * 7) 移除（取关）用户
 * 8) 获取关注的人的微博内容
 * 9) 测试
 *
 *
 */
public class weiboApp {

    private static Weibocontroller controller = new Weibocontroller();

    public static void main(String[] args) throws Exception {

        //1. 初始化
//            controller.init();

       //2. 添加关系表
//            controller.follow("1001","1002");
//            controller.follow("1001","1003");
//            controller.follow("1001","1004");

//        Hbase 查询结果：
//        ROW                            COLUMN+CELL
//        1001:Follow:1002              column=data:time, timestamp=1598784135932, value=1598784135610
//        1001:Follow:1003              column=data:time, timestamp=1598784135990, value=1598784135990
//        1001:Follow:1004              column=data:time, timestamp=1598784136003, value=1598784136003
//        1001:Follow:1005              column=data:time, timestamp=1598784420649, value=1598784205117
//        1002:BeFollowedBY:1001        column=data:time, timestamp=1598784135950, value=1598784135949
//        1003:BeFollowedBY:1001        column=data:time, timestamp=1598784135994, value=1598784135993
//        1004:BeFollowedBY:1001        column=data:time, timestamp=1598784136007, value=1598784136007
//        1005:BeFollowedBY:1001        column=data:time, timestamp=1598784429573, value=1598784428874

        //3. 取关
//        controller.unfollow("1001","1005");  //取关，删掉数据


        //4.  发布数据
//        controller.publish("1002","happy 10.1");
//        controller.publish("1002","happy 10.2");
//        controller.publish("1002","happy 10.3");
//        controller.publish("1002","happy 10.4");
//        controller.publish("1002","happy 10.5");
//        controller.publish("1002","happy 10.6");

        //todo  getWeiboByStarId("1001")


        //获取近期微博
        controller.getAllRecentWeibos("1001"); //fan: '1001'




    }

}
