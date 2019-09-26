package RDDToDataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Convert {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext("local", "caseclass", sparkConf)
    val sqlContext = new SQLContext(sc)

    val rawSales = sc.textFile("D:\\Spark\\Usecase\\SparkLoad\\salescsv.csv")

    // Easiest way to filter header / static and not efficient
    // val temp = rawSales.filter(!_.startsWith("TransactionID"))

    sc.setLogLevel("WARN")

    val noHeaderSalesRDD = rawSales.mapPartitionsWithIndex((sNo, pIntr) => {
      if (sNo == 0) pIntr.drop(1) else pIntr
    })

    val salesRDD = noHeaderSalesRDD.map( record => {
      val p = record.split(",")
      Sales(p(0).trim.toInt,p(1).trim.toInt,p(2).trim.toInt,p(3).trim.toDouble)
    })

    import sqlContext.implicits._
    val salesDF = salesRDD.toDF()

    //salesDF.printSchema()     Header is removed

    salesDF.show()
  }

}
