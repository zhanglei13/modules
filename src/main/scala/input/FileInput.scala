package input

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

// 实现文件数据源输入，生成DataFrame

object FileInput extends App {
  val conf = new SparkConf().setAppName("Test").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  //val path = "file:///opt/spark-1.4.1-bin-hadoop2.6/examples/src/main/resources/kv1.txt"
  val path = "file:///opt/spark-1.4.1-bin-hadoop2.6/examples/src/main/resources/people.json"

  val df = sqlContext.read.text(path)
  df.printSchema()
}
