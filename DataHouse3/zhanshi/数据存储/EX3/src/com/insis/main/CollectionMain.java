package com.insis.main;

import com.insis.utils.Config;

public class CollectionMain {
	public CollectionMain() {
	}

	public static void main(String[] args) {
		Collection collection = new Collection();
		collection.singleSource(Config.HOUR);
		//collection.singleConsumer(true);

//		 collection.multiSource(Config.HOUR);
		// collection.multiConsumer(false);
//		collection.singleStream(true);//消费并且存入hbase


	}

}
