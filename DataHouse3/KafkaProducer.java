package DataHouse3;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class KafkaProducer {

    public static void te(String[] args) throws Exception{

        long events = 10000;
        Random rnd = new Random();
        Properties props = new Properties();
        //配置kafka集群的broker地址，建议配置两个以上，以免其中一个失效，但不需要配全，集群会自动查找leader节点。
        props.put("metadata.broker.list", "172.31.42.152:9092,172.31.42.87:9092,172.31.42.214:9092");
        //配置value的序列化类
        //key的序列化类key.serializer.class可以单独配置，默认使用value的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);

        for (long nEvents = 0; nEvents < events; nEvents++) {

            long runtime = new Date().getTime();
            String ip = "192.168.2." + rnd.nextInt(255);
            String msg = runtime + ",www.example.com," + ip;
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("kafka_test", ip, msg);
            producer.send(data);
            System.out.println("send"+data);
        }

        producer.close();
    }


    public void sendData(String[] kafkaData) throws Exception{

        Properties props = new Properties();
        //配置kafka集群的broker地址，建议配置两个以上，以免其中一个失效，但不需要配全，集群会自动查找leader节点。
        props.put("metadata.broker.list", "172.31.42.152:9092,172.31.42.87:9092,172.31.42.214:9092");
        //配置value的序列化类
        //key的序列化类key.serializer.class可以单独配置，默认使用value的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);

        for (String line : kafkaData) {
            String msg = line;
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("kafka_userbehavior", null , msg);
            producer.send(data);
            System.out.println("send"+data);
        }

        producer.close();
    }
}
