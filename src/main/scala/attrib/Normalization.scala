package attrib

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.functions.{min, max, mean,stddev_pop}

// 实现某一列的归一化，归一化方式可选：最小-最大，z-score

object Normalization extends App {
  def normal(df:DataFrame,column:String,way:String): DataFrame={
    val (vMin, vMax,vMean,vStddev) = df.agg(min(column), max(column),mean(column),stddev_pop(column)).first match {
      case Row(x: Int, y: Int,z:Double,m:Double) => (x, y,z,m)
    }

    way match {
      case "minmax"=>df.withColumn(column+"_scaled", (df(column) - vMin) / (1.0*(vMax - vMin)))
      case "zscore"=>df.withColumn(column+"_scaled",(df(column) - vMean) /vStddev)
      case _=>df
    }
   // val vNormalized =(df(column) - vMin) / (1.0*(vMax - vMin)) // v normalized to (0, 1) range
//    df.withColumn(column+"_scaled", vNormalized)
  }
  val conf = new SparkConf().setAppName("modules").setMaster("spark://192.168.99.105:7077")
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)
  val df = sqlContext.read.format("jdbc").options(
    Map("url" -> "jdbc:mysql://192.168.99.1/test", "driver" -> "com.mysql.jdbc.Driver",
      "dbtable" -> "mt", "user" -> "root", "password" -> "cuiguangfan")
  ).load()
  df.collect().foreach(println)
  val newDf=normal(df,"c2","")
  val newDf1=normal(df,"c2","minmax")
  val newDf2=normal(df,"c2","zscore")
  newDf.collect().foreach(println)
  newDf1.collect().foreach(println)
  newDf2.collect().foreach(println)
}
