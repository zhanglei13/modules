package attrib

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

// 删除指定属性列

object DropColumns extends App{
  def drop(df:DataFrame,column:String): DataFrame={
    df.drop(column)
  }
  val conf = new SparkConf().setAppName("modules").setMaster("spark://192.168.99.105:7077")
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)
  val df = sqlContext.read.format("jdbc").options(
    Map("url" -> "jdbc:mysql://192.168.99.1/test", "driver" -> "com.mysql.jdbc.Driver",
      "dbtable" -> "mt", "user" -> "root", "password" -> "cuiguangfan")
  ).load()
  df.collect().foreach(println)
  val newDf=drop(df,"c3")
  newDf.collect().foreach(println)
}
