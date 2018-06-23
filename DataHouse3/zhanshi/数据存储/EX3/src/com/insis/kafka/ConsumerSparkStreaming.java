package com.insis.kafka;

import com.insis.dao.HbaseDao;
import com.insis.utils.ConfigRead;
import com.insis.utils.FileLengh;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author ZhuQichao
 * @create 2018/6/7 19:46
 **/
public class ConsumerSparkStreaming {

    public String getTopic() {
        return topic;
    }

    public int getIndex() {
        return index;
    }

    public String getGroupid() {
        return groupid;
    }

    public HbaseDao getDao() {
        return dao;
    }

    public void setDao(HbaseDao dao) {
        this.dao = dao;
    }

    public boolean isSave() {
        return isSave;
    }

    public void setSave(boolean save) {
        isSave = save;
    }

    private final String topic;
    private final int index;
    private final String groupid;

    private HbaseDao dao;
    private boolean isSave;
    public static String tableName = null;
    public static String[] fileds = null;

    public ConsumerSparkStreaming(String topic, int index, String groupid, boolean isSave) {
        this.topic = topic;
        this.index = index;
        this.groupid = groupid;
        this.isSave = isSave;
    }

    public static void getStreaming(ConsumerSparkStreaming cs) {
        System.out.println("streaming........");
        SparkConf sparkConf = new SparkConf().setAppName("EX3").setMaster("local[2]");
        //定义每两秒读一次数据
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(cs.topic, new Integer(1));
        String zk = ConfigRead.getProperty("zkConnectStream");
        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zk, cs.groupid, topicCountMap);
        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return stringStringTuple2._2;
            }
        });
        if (!cs.isSave) {
            lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
                @Override
                public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                    System.out.println("consume....");
                    List<String> ll = stringJavaRDD.collect();
                    for (String l : ll) {
                        System.out.println("consume " + l);
                        Thread.sleep(1000);
                    }
                }
            });
        }


        if (cs.isSave) {
            String[] cols = FileLengh.valueOf(cs.topic).getZd().split(",");//h获取TOPIC相应的字段数组
            int zdLen = cols.length;
            String[] col = new String[zdLen - 1];
            for (int i = 0; i < zdLen-1; i++) {
                col[i ] = "bean:" + cols[i];
            }
            fileds = col;
            try {
                tableName = ConfigRead.getProperty("hbase.namespace") + ":" + cs.topic;
                HbaseDao.createTable(tableName, fileds);
            } catch (IOException e) {
                e.printStackTrace();
            }

            lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
                @Override
                public void call(JavaRDD<String> stringJavaRDD) throws Exception {

                    stringJavaRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
                       @Override
                        public void call(Iterator<String> stringIterator) throws Exception {
                            org.apache.hadoop.hbase.client.Connection con = HbaseDao.getConnection();   //这个是ok
                            while (stringIterator.hasNext()) {
                                String line = stringIterator.next();
                                System.out.println(line);
                                String[] str = line.split("\001");
                                String row = str[str.length-1];
                                int len = line.split("\001").length;
                                String[] value = new String[len - 1];
                                for (int i = 0; i < str.length-1; i++) {
                                    value[i ] = str[i];//日期做主键
                                }

                                //HbaseDao.addRecord(tableName, row, fileds, value);
                                HbaseDao.addRecordCon(tableName,row,fileds, value,con);
                            }
                        }
                    });
                }
            });
        }

        jssc.start();
        jssc.awaitTermination();

    }

}
