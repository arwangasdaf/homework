package DataHouse3

import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTablePool, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object launchStreaming {
  def main(args: Array[String]): Unit = {
    val sprakConf = new SparkConf().setAppName("UserBehavior_Spark")
    //此处在idea中运行时请保证local[2]核心数大于2
    sprakConf.setMaster("local[3]")
    val ssc = new StreamingContext(sprakConf, Seconds(10))

    val brokers = "172.31.42.152:9092,172.31.42.87:9092,172.31.42.214:9092"
    val topics = "kafka_userbehavior"
    val topicSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)



    //
    val lines = messages.map(_._2).map { case line =>
      val array = line.split("@")

      // (behaviortime , userid , behavior , aid)

      (array(3) , array(0) , array(1) , array(2))

    }


    lines.repartition(5).foreachRDD( rdd =>
       process(rdd)
    )
    ssc.start()
    ssc.awaitTermination()
  }


  // 对每个Rdd进行处理
  def process(value: RDD[(String,String,String,String)]): Unit = {
      processPatition(value)
  }



  // 对每个partition进行处理
  def processPatition(value: RDD[(String,String,String,String)]): Unit ={
    value.foreachPartition{
      case triple=>{

        val myConf = HBaseConfiguration.create()
        myConf.set("hbase.zookeeper.quorum", "172.31.42.152,172.31.42.87,172.31.42.214")
        myConf.set("hbase.zookeeper.property.clientPort", "2181")
        myConf.set("hbase.defaults.for.version.skip", "true")
        val pool = new HTablePool(myConf, 100)
        val myTable = pool.getTable("wangtao:userbehavior")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5 * 1024 * 1024)


        triple.foreach{
          case item=>{
            val behavior_time = item._1
            val userid = item._2
            val behavior = item._3
            val aid = item._4


            // write to hbase
            val p = new Put(Bytes.toBytes(behavior_time+"@"+userid))

            p.add(Bytes.toBytes("Info"), Bytes.toBytes(aid), Bytes.toBytes(behavior))
            //
            myTable.put(p)
            //

            System.out.println("----------Insert to hbase-----------------"+item._1.toString)
          }
        }

        myTable.flushCommits()
      }
    }
  }




}
