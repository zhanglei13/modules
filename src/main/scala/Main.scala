import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by zhanglei on 16-3-29.
 */
object Main {
  def main(args: Array[String]): Unit = {
    // initial context
    val conf = new SparkConf().setAppName("modules").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    // initial fields

    // body
  }
}
