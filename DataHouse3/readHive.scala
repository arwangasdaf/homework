package DataHouse3

import java.util
import java.util.Properties

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/*
* @object: readHive
* @author: wangtao
* @describe: 从Hive里读取小时聚合的数据做天聚合
* */

object readHive {
  // passage类
  case class passage(time:String , articleId:String , domain:String , behavior:String  , num:String)

  def main(args: Array[String]): Unit = {
    //init
    val sparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("readHIVE")
    val sc = new SparkContext(sparkConf)
    //
    System.setProperty("spark.driver.memory", "1G")

    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    // 天聚合
    //ArticleDayAggregete(hiveContext)


    // 周报表
    weekReport(hiveContext)


    sc.stop()
  }


  /*
  * @method: ArticleDayAggregete
  * */
  def ArticleDayAggregete(hiveContext:HiveContext):Unit={
    import hiveContext.implicits._
    hiveContext.sql("use Data")


    // (time , articleId , domin , behavior , num)
    val articleRDD = hiveContext.sql("select * from Article_Info").rdd
        .map{case line=>((line.get(0).toString.substring(0,8) ,line.get(1).toString , line.get(2).toString , line.get(3).toString) , line.get(4).toString.toInt)}

    /*val resultRDD = articleRDD.reduceByKey(_ + _).map(line=>{
      passage(line._1._1 , line._1._2 , line._1._3 , line._1._4 , line._2.toString)
    })


    val url: String = "jdbc:mysql://172.31.42.214:3306/ex3"
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "cluster")
    //jdbcDF.write.mode(SaveMode.Overwrite).jdbc(url,"baike_pages",prop)
    resultRDD.toDF().write.mode(SaveMode.Append).jdbc(url, "Article_Info", prop)*/



    articleRDD.reduceByKey(_ + _).repartition(3).foreachPartition{
      case triple=>{
        val du = new DButil()
        val conn = du.getConnection
        println("--------------get connection-------------")
        triple.foreach{
          case item=>{
            val statement = conn.prepareStatement("insert into Article_Info values (?,?,?,?,?)")
            statement.setString(1 , item._1._1.toString())
            statement.setString(2 , item._1._2.toString())
            statement.setString(3 , item._1._3.toString())
            statement.setString(4 , item._1._4.toString())
            statement.setString(5 , item._2.toString())
            statement.executeUpdate()
            statement.close()
            println("---------------insert--------------")
          }
        }
        conn.close()
      }
    }
  }


  /*
  * @method: weekReport: 对一周内对数据进行统计
  * @param: hiveContext
  * @return: void
  * */

  def weekReport(hiveContext: HiveContext):Unit={
    import hiveContext.implicits._
    hiveContext.sql("use Data")


    // (time , useid, domin , num)
    val articleRDD = hiveContext.sql("select * from Act_Num").rdd
      .map{case line=>(line.get(0).toString ,line.get(1).toString , line.get(2).toString , line.get(3).toString)}


    // (time  , userid , num)
    val tempRDD = articleRDD.map(line=>( line._1.split(" ")(0) , line._2 , line._4.toInt))

    // 统计用户每天最活跃和最冷的小时情况
    // (day , hour , num) 正向排序
    val min_max = tempRDD.groupBy(line => line._1)

    val minRDD= min_max.map{
        case subG =>{
          val day = subG._1

          val min = subG._2.toList.sortBy(_._3).take(1)

          min
        }
      }

    minRDD.saveAsTextFile("hdfs://172.31.42.151:9000/wangtao/dataEx/weekReport/LeastActive")


    val maxRDD= min_max.map{
      case subG =>{
        val day = subG._1


        val max = subG._2.toList.sortBy(_._3)(Ordering.Int.reverse).take(1)

         max
      }
    }

    maxRDD.saveAsTextFile("hdfs://172.31.42.151:9000/wangtao/dataEx/weekReport/MostActive")


    // 统计一周内的期望和标准差

    // (time , num) map => (zhou , num)

    // 1、先计算一周内的天数(2015-08-01) (7 , 01)
    // 2、计算一周内的平均值
    // 3、计算一周

    // weekdayRDD (7 , 5) 一周有几天
    val weekdayRDD = tempRDD.map(line => (TranUtil.tran(line._1) , line._1.split("-")(2))).distinct()
      .map(line => (line._1 , 1)).reduceByKey(_ + _)

    // weekNumRDD (7 , 7878) 一周有多少点击次数
    val weekNumRDD = tempRDD.map(line=>(TranUtil.tran(line._1) , line._3)).reduceByKey(_+_)

    val weektianshu = new util.HashMap[String , Int]()
    weekdayRDD.collect().foreach(
      line=>{
        weektianshu.put(line._1 , line._2)
      }
    )


    val averageRDD = weekNumRDD.join(weekdayRDD).map{
      case line=>{
        val week = line._1
        val weeknum = line._2._1.toDouble
        val tianshu = line._2._2.toDouble

        (week , weeknum/tianshu)
      }
    }

    averageRDD.saveAsTextFile("hdfs://172.31.42.151:9000/wangtao/dataEx/weekReport/Average")

    val averageweek = new util.HashMap[String , Double]()

    averageRDD.collect().foreach(
      line =>{
        averageweek.put(line._1 , line._2)
      }
    )


    val daynumRDD = tempRDD.map(line=> (line._1 , line._3)).reduceByKey(_ + _)

    val biaozhunchaRDD = daynumRDD.map(line=>(TranUtil.tran(line._1) , line._2)).groupBy(line => line._1).map{
      case line=>{
        val week = line._1
        val tianshu = weektianshu.get(week)
        val average = averageweek.get(week)

        val x = line._2.map(line => Math.pow(line._2.toDouble - average ,2)).reduce(_ + _)

        val biaozhuncha = Math.sqrt(x / tianshu)

        (week , biaozhuncha)
      }
    }

    biaozhunchaRDD.saveAsTextFile("hdfs://172.31.42.151:9000/wangtao/dataEx/weekReport/BiaoZhunCha")

    // 计算最活跃的前10名用户

    // (周 , user , num)
    val tenUserRDD = tempRDD.map{line =>((TranUtil.tran(line._1) , line._2 ), line._3)}.reduceByKey(_ + _)
      .map(line =>( line._1._1 , line._1._2 , line._2)).groupBy(line => line._1)
      .map{
        case line=>{
          val week = line._1
          val userGroup = line._2.toList.sortBy(_._3)(Ordering.Int.reverse).take(10).map(line => (line._2 , line._3))

          (week , userGroup)

        }
      }

    tenUserRDD.saveAsTextFile("hdfs://172.31.42.151:9000/wangtao/dataEx/weekReport/User10")

    // 计算最活跃的前10个领域

    // (周 , domain , num)

    val domaintenRDD = articleRDD.map(line => ((TranUtil.tran(line._1.split(" ")(0)) , line._3) , line._4.toInt)).reduceByKey(_ + _)
      .map(line=>(line._1._1 , line._1._2 , line._2)).groupBy(line=> line._1).map{
      case line=>{
        val week = line._1
        val domainGroup = line._2.toList.sortBy(_._3)(Ordering.Int.reverse).take(10).map(line=>(line._2 , line._3))

        (week , domainGroup)
      }
    }

    domaintenRDD.saveAsTextFile("hdfs://172.31.42.151:9000/wangtao/dataEx/weekReport/Domain10")


  }

}
