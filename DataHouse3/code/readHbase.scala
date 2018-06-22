package DataHouse3

import java.util.Properties

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/*
* @object: readHbase
* @author: wangtao
* @datatime: 2018-6-13
* @describe: 读取hbase的数据
* */
object readHbase {

  // passage类
  case class passage(time:String , passageId:String , domain:String , behavior:String  , num:String)
  // active类
  case class active(time:String , userId:String , domain:String , num:String)
  def main(args: Array[String]): Unit = {
    //init
    val sparkConf = new SparkConf()
                    .setMaster("local[3]")
                    .setAppName("readHbase")
    val sc = new SparkContext(sparkConf)
    //
    System.setProperty("spark.driver.memory", "1G")

    //

    val startRowkey="2015-05-08 13:13:19.0@U0155827"
    val endRowkey="2015-05-31 23:59:47.0@U0042225"
    //开始rowkey和结束一样代表精确查询某条数据

    //组装scan语句

    //打印扫描的数据总量
    val config = HBaseConfiguration.create
    val table_name = "wangtao:userbehavior"
    config.set("hbase.zookeeper.quorum", "172.31.42.152,172.31.42.87,172.31.42.214")
    config.set("hbase.zookeeper.property.clientPort", "2181")
    config.set("hbase.defaults.for.version.skip", "true")
    config.set(TableInputFormat.INPUT_TABLE, table_name)

    config.set(TableInputFormat.SCAN_ROW_START,startRowkey)
    config.set(TableInputFormat.SCAN_ROW_STOP,endRowkey)

    val hbaseRDD = sc.newAPIHadoopRDD(config, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val resultRDD = hbaseRDD.map(tuple => tuple._2.raw())
    //resultRDD.count()


    //(time@userid,passageid，behavior)=>(passageid , userid ， time , behavior)
    // 时间处理之后规范到2015050101 小时级别
    /*val DataRdd = resultRDD.flatMap { res =>
      res.map(i => (Bytes.toString(i.getRow) , Bytes.toString(i.getQualifier) , Bytes.toString(i.getValue)))
    }.map{
      case line=>{
        val timeAndUserid = line._1.split("@")
        var time = timeAndUserid(0).replace("-", "").replace(" ", "").replace(":", "").substring(0,10)
        val userid = timeAndUserid(1)
        val passageid = line._2
        val behavior = line._3
        (passageid , userid , time , behavior)
      }
    }*/

    // 时间处理之后规范到2015050101 小时级
    val DataRdd = resultRDD.flatMap { res =>
      res.map(i => (Bytes.toString(i.getRow) , Bytes.toString(i.getQualifier) , Bytes.toString(i.getValue)))
    }.map{
      case line=>{
        val timeAndUserid = line._1.split("@")
        var time = timeAndUserid(0).split(":")(0)
        val userid = timeAndUserid(1)
        val passageid = line._2
        val behavior = line._3
        (passageid , userid , time , behavior)
      }
    }


    // 计算passage的活跃度，按照小时聚合

    //hourAggregete(DataRdd,sc)

    // 计算用户行为的活跃

    hourActiveAggregete(DataRdd , sc)


    // 结束 sparkContext
     sc.stop()


  }

  /*
  *@method: hourAggregete:文档小时聚合
  *@param: DataRDD: 原始动态数据
  *@return: void
  * */

  def hourAggregete(DataRDD:RDD[(String , String , String  , String)] , sc: SparkContext):Unit={
    // (passageid , (time , behavior , num)

    val passageRDD = DataRDD.map(line => ((line._1 , line._3 , line._4) , 1)).reduceByKey(_ + _)
      .map(line => (line._1._1 , (line._1._2 , line._1._3 , line._2)))

    // 读取mysql里的article的数据
    val sqlContext = new SQLContext(sc)

    var url = "jdbc:mysql://172.31.42.214:3306/ex3?user=root&password=cluster"

    val prop = new Properties()

    val df  = sqlContext.read.jdbc(url , "articleInfo" , prop)

    df.registerTempTable("articleInfo")

    val articleInfoRDD = sqlContext.sql("select * from articleInfo").rdd
      .map{case line=>{
          val passageid = line.get(0).toString
          val Type = line.get(1).toString
          val IsTop = line.get(2).toString
          val status = line.get(3).toString
          val domin = line.get(4).toString
          (passageid , domin)
      }
      }


    // join
    // (datatime , passageid , domin ,  behavior , num)
    val resultRDD = passageRDD.join(articleInfoRDD).map(line => passage(line._2._1._1 , line._1 , line._2._2 , line._2._1._2 , line._2._1._3.toString))


    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import hiveContext.implicits._
    hiveContext.sql("use Data")

    resultRDD.toDF().insertInto("Article_Info")

  }


  /*
  * @method : hourActiveAggregete: 计算用户活跃程度，首先按小时聚合
  * @param: DataRdd: 动态数据 , sc: sparkContext
  * @return: void
  * */

  def hourActiveAggregete(DataRDD: RDD[(String, String, String, String)], sc: SparkContext):Unit={
    // (passageid , (userid , time , behavior , num)

    val passageRDD = DataRDD.map(line => (line._1 , (line._2 , line._3 , line._4)))

    // 读取mysql里的article的数据
    val sqlContext = new SQLContext(sc)

    var url = "jdbc:mysql://172.31.42.214:3306/ex3?user=root&password=cluster"

    val prop = new Properties()

    val df  = sqlContext.read.jdbc(url , "articleInfo" , prop)

    df.registerTempTable("articleInfo")

    val articleInfoRDD = sqlContext.sql("select * from articleInfo").rdd
      .map{case line=>{
        val passageid = line.get(0).toString
        val Type = line.get(1).toString
        val IsTop = line.get(2).toString
        val status = line.get(3).toString
        val domin = line.get(4).toString
        (passageid , domin)
      }
      }


    // join
    // ((datatime , user ,   domin) , 99)
    val resultRDD = passageRDD.join(articleInfoRDD).map(line =>((line._2._1._2  , line._2._1._1, line._2._2) , 1)).reduceByKey(_ + _)
      .map(line => active(line._1._1 , line._1._2 , line._1._3 , line._2.toString))

    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import hiveContext.implicits._
    hiveContext.sql("use Data")

    resultRDD.toDF().insertInto("Act_Num")
  }


}
