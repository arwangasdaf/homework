package com.insis.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class KafkaPartitioner implements Partitioner {

	public KafkaPartitioner() {
	}

	/**
	 * 该类可以使用自定义分区器
	 * 
	 * @param verifiableProperties
	 */
	public KafkaPartitioner(VerifiableProperties verifiableProperties) {

	}

	@Override
	public int partition(Object key, int numPartitions) {
		try {
			long partitionNum = Long.parseLong((String) key);
			return (int) Math.abs(partitionNum % numPartitions);
		} catch (Exception e) {
			return Math.abs(key.hashCode() % numPartitions);
		}
	}

}
