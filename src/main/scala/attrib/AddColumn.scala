package attrib

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

// 新增属性列

object AddColumn extends App {
  val conf = new SparkConf().setAppName("Test").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val column = "new"

  val df = sqlContext.read.format("jdbc").options(
    Map("url" -> "jdbc:mysql://133.133.2.150/test", "driver" -> "com.mysql.jdbc.Driver",
      "dbtable" -> "student", "user" -> "root", "password" -> "123456")
  ).load()

  val rdd = df.map(r => Row(r(0), r(1), r(0).asInstanceOf[Int]+1))

}
