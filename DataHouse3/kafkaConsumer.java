package DataHouse3;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class kafkaConsumer {

    public static void main(String[] args){
        Properties props = new Properties();
        props.put("zookeeper.connect", "172.31.42.152:2181,172.31.42.214:2181,172.31.42.87:2181");
        props.put("group.id", "123");
        props.put("auto.offset.reset","smallest");

        // Create the connection to the cluster
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

        HashMap<String, Integer> map = new HashMap<String, Integer>();
        map.put("kafka_test", 1);
        Map<String, List<KafkaStream<byte[] , byte[]>>> topicMessageStreams =
                consumerConnector.createMessageStreams(map);
        List<KafkaStream<byte[] , byte[]>> streams = topicMessageStreams.get("kafka_test");

        for (final KafkaStream<byte[] , byte[]> stream : streams) {
            for (MessageAndMetadata<byte[],byte[]> msgAndMetadata : stream) {
                // process message (msgAndMetadata.message())
                System.out.println("topic: " + msgAndMetadata.topic());
                String message = new String(msgAndMetadata.message());
                System.out.println("message content: " + message);
                System.out.println("-----------------------------");
            }
        }
    }
}
