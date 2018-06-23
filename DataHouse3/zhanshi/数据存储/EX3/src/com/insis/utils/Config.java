package com.insis.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * 配置文件类，存储静态变量，以及读取和修改.properties配置文件
 * 
 * @author czisok
 * 
 */
public class Config {

	/*-----------------------MySQL config----------------------------*/
	public static final String URL = "jdbc:mysql://172.31.42.214:3306/test?userUnicode=true&amp;characterEncoding=UTF-8 &amp;zeroDateTimeBehavior=converToNull";
	public static final String USERNAME = "root";
	public static final String PWD = "cluster";

	/*-----------------------kafka config----------------------------*/
//	public final static String zkConnect = "172.31.42.152,172.31.42.214,172.31.42.87";
//	public final static String kafkaUrl = "172.31.42.152:9092,172.31.42.214:9092,172.31.42.87:9092";
	public final static String zkConnect = "172.31.42.152,172.31.42.87";
	public final static String kafkaUrl = "172.31.42.152:9092,172.31.42.87:9092";
	public final static String groupId = "group1";
	public final static int kafkaServerPort = 9092;
	public final static int kafkaProducerBufferSize = 64 * 1024;
	public final static int connectionTimeOut = 20000;
	public final static int reconnectInterval = 10000;
	public final static String topic0 = "test";
	public final static String topic = "userBehavior_zqc";
	public final static String topic1 = "topic1";
	public final static String topic2 = "topic2";
	public final static String topic3 = "topic3";
	public final static String clinetID = "SimpleConsumerDemoClient";

	/*-----------------------other config----------------------------*/
	public static final String SECOND = "second";
	public static final String HOUR = "hour";
	public static String stoptime = "2015-01-01 00:00:40.0";
	public static int offset = 1;
	public static String fileLastTime = "2015-01-31 23:59:54.0";

	public static String filePath0 = "./data/test";
	public static String filePath1 = "./data/userBehavior";
	public static String filePath2 = "userBehavior/userBehavior-sample2";

	/**
	 * 存储文件中读取到得配置信息
	 */


	public static void main(String[] args) {
		//setProperty("cz", "3678");

		System.out.println(Config.filePath0);

	}
}
