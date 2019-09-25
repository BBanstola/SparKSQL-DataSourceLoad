import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object LoadJson {

  def main(args: Array[String]): Unit = {

    val input = "D:\\Spark\\Usecase\\SparkLoad\\salesjson.json"

    val sparkConf = new SparkConf()
    val sc = new SparkContext("local", "JSONfile", sparkConf)
    val sqlContext = new SQLContext(sc)

    sc.setLogLevel("WARN")

    val salesJSON = sqlContext.read.format("json").load(input)

    salesJSON.printSchema()

    salesJSON.show(false)


  }

}
