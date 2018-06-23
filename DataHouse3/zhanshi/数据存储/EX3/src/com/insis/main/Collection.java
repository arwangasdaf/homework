package com.insis.main;


import com.insis.kafka.ConsumerSparkStreaming;
import com.insis.kafka.LogConsumer;
import com.insis.kafka.LogProducer;
import com.insis.utils.Config;

/**
 * 日志采集和消费
 * 
 * @author czisok
 * 
 */
public class Collection {
	private String topic = Config.topic;//设置topic名称

	public Collection() {
	}

	/**
	 * 单数据源消生产者
	 * 
	 * @param interval
	 *            - 时间粒度
	 */
	public void singleSource(String interval) {
		LogProducer producer = new LogProducer(topic, interval, Config.filePath1);
		producer.start();
	}

	/**
	 * 多数据源推送消息，本例使用三个消费者
	 */
	public void multiSource(String interval) {
		LogProducer producer0 = new LogProducer(topic, interval,
				Config.filePath0);
		producer0.start();
		LogProducer producer1 = new LogProducer(topic, interval,
				Config.filePath1);
		producer1.start();

		LogProducer producer2 = new LogProducer(topic, interval,
				Config.filePath2);
		producer2.start();
	}

	/**
	 * 单个消费者
	 * 
	 * @param isSave
	 *            - 是否存储到数据库
	 */
	public void singleConsumer(boolean isSave) {
		LogConsumer consumer1 = new LogConsumer(topic, 0, "group1", isSave);
		consumer1.start();
	}

	/**
	 * 多个消费者消费数据，本例使用三个消费者
	 * 
	 * @param isSave
	 *            - 是否存储到数据库
	 */
	public void multiConsumer(boolean isSave) {
		LogConsumer consumer1 = new LogConsumer(topic, 0, "group1", isSave);
		consumer1.start();

		LogConsumer consumer2 = new LogConsumer(topic, 1, "group1", isSave);
		consumer2.start();

		LogConsumer consumer3 = new LogConsumer(topic, 2, "group1", isSave);
		consumer3.start();
	}

	public  void singleStream(boolean isSave){
		//String topic, int index, String groupid, boolean isSave
		ConsumerSparkStreaming cs = new ConsumerSparkStreaming(topic,0,"group1",isSave);
		ConsumerSparkStreaming.getStreaming(cs);
	}

}
