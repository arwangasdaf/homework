package com.insis.kafka;


import com.insis.dao.HbaseDao;
import com.insis.utils.Config;
import com.sun.java.swing.plaf.windows.WindowsBorders;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class LogConsumer extends Thread {
	private final ConsumerConnector consumer;
	private final String topic;
	private final int index;
	private final String groupid;

	private HbaseDao dao;
	private boolean isSave = false;

	/**
	 * 
	 * @param topic
	 *            - 要消费的话题
	 * @param index
	 *            - 为消费者指定一个id
	 * @param groupid
	 *            - 为消费者指定一个组别
	 */
	public LogConsumer(String topic, int index, String groupid, boolean isSave) {
		consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(createConsumerConfig(groupid));
		this.topic = topic;
		this.index = index;
		this.groupid = groupid;
		this.isSave = isSave;

	}

	/**
	 * 使用配置文件，创建一个消费者配置项
	 * 
	 * @param groupid
	 *            - 该消费者所属组别
	 * @return
	 */
	private static ConsumerConfig createConsumerConfig(String groupid) {
		Properties props = new Properties();
		props.put("zookeeper.connect", Config.zkConnect);
		props.put("group.id", groupid);
		props.put("zookeeper.session.timeout.ms", "40000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		return new ConsumerConfig(props);

	}

	@Override
	public void run() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));

		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);// consumerMap.get(topic)返回一个list，只能get(0),
		ConsumerIterator<byte[], byte[]> it = stream.iterator();

		if (isSave) {

		}//创建表

		while (it.hasNext()) {
			MessageAndMetadata<byte[], byte[]> mam = it.next();
			String mes = new String(mam.message());
			System.out.println(groupid + "  cosumer" + index + " receive- 分区【"
					+ mam.partition() + "】-" + "offset: " + mam.offset() + "--"
					+ mes);

			if (isSave) {

			}

			//向表中插入数据
//			try {
//				System.out.println("sleep ");
//				sleep(1000);
//			} catch (InterruptedException e) {
//
//			}
		}
	}
}
