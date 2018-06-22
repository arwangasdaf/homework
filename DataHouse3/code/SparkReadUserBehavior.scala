package DataHouse3

import org.apache.spark.{SparkConf, SparkContext}


/*
* @object: SparkReadUserBehavior
* @author: wangtao
* @describe: 读取userbehavior的数据，按照时间排序，通过kafka发送
* */
object SparkReadUserBehavior {

  def main(args: Array[String]): Unit = {
    // inti sparkContext
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("ReadUserBehavior")
    val sc = new SparkContext(sparkConf)

    // read userBehavior
    val userBehavior = sc.textFile("/Users/wangtao/Downloads/data/dataHouse/userBehavior/userBehavior")
    //
    val userBehaviorRDD = userBehavior.map(
      line=>{
        (line.split("\001")(3) , line.split("\001")(0)+"@"+line.split("\001")(1)+"@"+line.split("\001")(2))
      }
    ).sortBy(line => (line._1 , true))
      .map(line=>
       line._2+"@"+line._1)

    // 排序后的数据
    val kafkaData = userBehaviorRDD.collect()

    // kafka 生产者发送数据
    val kafkaProducer = new KafkaProducer()

    try {
      kafkaProducer.sendData(kafkaData)
    }catch {
      case e:Exception => {
        e.printStackTrace()
      }
    }


    // stop
    sc.stop()
  }



}
