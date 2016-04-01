package attrib

import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types.{StructType,StructField,StringType};
// 新增属性列

object AddColumn extends App {
  def add(sparkContext:SparkContext,df:DataFrame,column:String,from:String): DataFrame= {
    //先将df转换成RDD，df.rdd，然后再利用stackoverflow上说的方法进行操作
//    val list:List[String]=List(column)
//    val schema =new StructType(Array(StructField(column,StringType,nullable = true)))
//      StructType(
//        list.map(fieldName => StructField(fieldName, StringType, true)))
//    val rowsRDD=sparkContext.parallelize(List.fill(df.count().toInt)("1").map(p => Row(p)))
//    val tempDF=sqlContext.createDataFrame(rowsRDD, schema)

//    df.select("*").show()
    df.withColumn(column, df(from))


  }
  val conf = new SparkConf().setAppName("modules").setMaster("spark://192.168.99.105:7077")
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)
  val df = sqlContext.read.format("jdbc").options(
    Map("url" -> "jdbc:mysql://192.168.99.1/test", "driver" -> "com.mysql.jdbc.Driver",
      "dbtable" -> "mt", "user" -> "root", "password" -> "cuiguangfan")
  ).load()
  df.foreach(println)
  val mnewDf=add(sc,df,"c4","c1")
  mnewDf.collect().foreach(println)
  mnewDf.printSchema()
}
