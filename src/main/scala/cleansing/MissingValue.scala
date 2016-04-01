package cleansing

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.DataFrame

// 缺失值清理，方式：清除/默认/预测

object MissingValue {
  def dropDuplicates(df:DataFrame,columns:String): DataFrame={
    columns match {
      case "*"=>df.dropDuplicates()
      case _=>df.dropDuplicates(columns.split(" "))//以空格分割
    }
  }
  val conf = new SparkConf().setAppName("modules").setMaster("spark://192.168.99.105:7077")
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)
  val df = sqlContext.read.format("jdbc").options(
    Map("url" -> "jdbc:mysql://192.168.99.1/test", "driver" -> "com.mysql.jdbc.Driver",
      "dbtable" -> "mt", "user" -> "root", "password" -> "cuiguangfan")
  ).load()
  df.collect().foreach(println)
  val newDf=dropDuplicates(df,"c3")
  newDf.collect().foreach(println)
}
