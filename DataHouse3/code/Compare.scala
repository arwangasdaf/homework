package DataHouse3

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/*
* @object: Compare
* @author: wangtao
* @datatime: 2018-6-16
* @describe: 针对一种指标，比较从mysql和从hive里读取比较性能(这里选取统计每天不同领域的文章被点击的次数作为比较对象)
* */

object Compare {

   /*
   * @method: main
   * @param: args: Array[String]
   * @reurn: Unit
   * */
  def main(args: Array[String]): Unit = {
    //init
    val sparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("readHIVE")
    val sc = new SparkContext(sparkConf)
    //
    System.setProperty("spark.driver.memory", "1G")



    // 从HIVE中读取

    //readHIVE(sc)

    // 从MYSQL中读取

    readMYSQL(sc)

    // stop sc

    sc.stop()

  }


  /*
  * @method: readHIVE: 从HIVE中读取数据进行计算
  * @param: sc:SparkContext
  * @return: Unit
  * */
  def readHIVE(sc:SparkContext):Unit={

    // init hiveContext
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    import hiveContext.implicits._
    hiveContext.sql("use Data")


    // (time , useid, domin , num)
    val articleRDD = hiveContext.sql("select * from Act_Num").rdd
      .map{case line=>(line.get(0).toString ,line.get(1).toString , line.get(2).toString , line.get(3).toString)}


    // (time  , userid , num)
    val tempRDD = articleRDD.map(line=>( (line._1.split(" ")(0) , line._3) , line._4.toInt)).reduceByKey(_+_)


    tempRDD.foreachPartition{
      case triple=>{
        val du = new DButil()
        val conn = du.getConnection
        println("--------------get connection-------------")
        triple.foreach{
          case item=>{
            val statement = conn.prepareStatement("insert into domin_day values (?,?,?)")
            statement.setString(1 , item._1._1.toString())
            statement.setString(2 , item._1._2.toString())
            statement.setString(3 , item._2.toString())
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
   * @method: readMYSQL: 从MYSQL中读取数据进行计算
   * @param: sc:SparkContext
   * @return: Unit
   * */
  def readMYSQL(sc:SparkContext):Unit={

    // 读取mysql里的article的数据
    val sqlContext = new SQLContext(sc)

    var url = "jdbc:mysql://172.31.42.214:3306/ex3?user=root&password=cluster"

    val prop = new Properties()

    val df  = sqlContext.read.jdbc(url , "Article_Info" , prop)

    df.registerTempTable("articleInfo")

    val articleInfoRDD = sqlContext.sql("select * from articleInfo").rdd
      .map{case line=>{
        val time = line.get(0).toString
        val domin = line.get(2).toString
        val num = line.get(4).toString.toInt
        ((time , domin) , num)
      }
      }

    val tempRDD = articleInfoRDD.reduceByKey(_+_)




    tempRDD.foreachPartition{
      case triple=>{
        val du = new DButil()
        val conn = du.getConnection
        println("--------------get connection-------------")
        triple.foreach{
          case item=>{
            val statement = conn.prepareStatement("insert into domin_day values (?,?,?)")
            statement.setString(1 , item._1._1.toString())
            statement.setString(2 , item._1._2.toString())
            statement.setString(3 , item._2.toString())
            statement.executeUpdate()

            println(item._2.toString)
            statement.close()
            println("---------------insert--------------")
          }
        }
        conn.close()
      }
    }

  }



}
