package input

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

// 实现jdbc连接数据库生成DataFrame

object DBInput extends App {
  val conf = new SparkConf().setAppName("Test").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val url = "jdbc:mysql://133.133.2.150/test"
  val driver = "com.mysql.jdbc.Driver"
  val dbtable = "student"
  val user = "root"
  val password = "123456"

  val rdd = sqlContext.read.format("jdbc").options(
    Map("url" -> url, "driver" -> driver, "dbtable" -> dbtable,
    "user" -> user, "password" -> password)
  ).load()

  rdd.show()
}
