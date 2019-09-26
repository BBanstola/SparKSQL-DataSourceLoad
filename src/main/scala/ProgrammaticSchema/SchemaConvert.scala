package ProgrammaticSchema

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object SchemaConvert {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext("local", "programmativeschema", sparkConf)
    val sqlContext = new SQLContext(sc)

    val salesSchema = StructType(Array(
      StructField("TransactionID", IntegerType, true),
      StructField("CustomerID", IntegerType, true),
      StructField("ItemID", IntegerType, true),
      StructField("AmountPaid", DoubleType, true)
    ))

    val rawRDD = sc. textFile("D:\\Spark\\Usecase\\SparkLoad\\salescsv.csv")


    sc.setLogLevel("WARN")

    val noHeaderSalesRDD = rawRDD.mapPartitionsWithIndex((sNo, pIntr) => {
      if (sNo == 0) pIntr.drop(1) else pIntr
    })

    val rowRDD = noHeaderSalesRDD.map(_.split(",")).map( p => {
      Row(p(0).trim.toInt,p(1).trim.toInt,p(2).trim.toInt,p(3).trim.toDouble)
    })

    val salesDF = sqlContext.createDataFrame(rowRDD, salesSchema)

    salesDF.show()

  }
}
