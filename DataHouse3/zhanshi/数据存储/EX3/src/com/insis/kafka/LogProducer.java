package com.insis.kafka;


import com.insis.utils.Config;
import com.insis.utils.DateTools;
import com.insis.utils.PreFile;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class LogProducer extends Thread {

	private final kafka.javaapi.producer.Producer<String, String> producer;
	private final String topic;
	private Properties props = new Properties();
	private String timeInterval = Config.HOUR;// 时间粒度
	private String filePath = "";
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");// sdf是非线程安全的

	/**
	 * 
	 * @param topic
	 *            - 生产话题
	 * @param timeInterval
	 *            - 时间粒度
	 * @param filePath
	 *            - 要消费的文件路径
	 */
	public LogProducer(String topic, String timeInterval, String filePath) {
		props.put("serializer.class", StringEncoder.class.getName());
		props.put("metadata.broker.list", Config.kafkaUrl);
		props.put("partitioner.class",
				"com.insis.kafka.KafkaPartitioner");
		producer = new Producer<String, String>(new ProducerConfig(props));
		this.topic = topic;
		this.timeInterval = timeInterval;
		this.filePath = filePath;
	}

	@Override
	public void run() {
		/*----------------down code: 计算时间-----------------*/
		Date startTime = new Date();
		/*----------------  up code: 计算时间-----------------*/
		int messageNo = 1;
		boolean isLast = false;
		while (true) {
			List<String> result = PreFile.readFile(filePath, Config.offset,
					timeInterval);
			for (String s : result) {
				if (s == null) {
					/*--------------down code:如果读到文件末尾-----------------*/
					Date now = new Date();
					DateTools.calEscapeTime(now.getTime() - startTime.getTime());
					isLast = true;
					break;
					/*--------------  up code:如果读到文件末尾-----------------*/
				}
				producer.send(new KeyedMessage<String, String>(topic, messageNo
						+ "", s));
				messageNo++;
				System.out.println("send:" + s);
			}
			if (isLast) {
				break;
			}
			// System.out.println("send message num: " + result.size());

			Config.offset += result.size();

			// try {
			// Thread.sleep(1500);
			// } catch (InterruptedException e) {
			// e.printStackTrace();
			// }
		}
	}
}
