package attrib

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

// 删除指定属性列

object DropColumns {
  val conf = new SparkConf().setAppName("Test").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
}
