package ProgrammaticSchema

import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

object SchemaConvertAlternativeWay {

  val typeToSparkType = Map("Int" -> IntegerType, "double" -> DoubleType)

  def getSchemaFromColumnType(fileName: String) ={

    val columnNameTypes = scala.io
          .Source
          .fromFile(fileName)
          .getLines.toArray
          .map(_.trim)
          .map(e => {
            val a = e.split(" ")
            (a(0),a(1))
          })

    val columnFields = columnNameTypes.map(t => StructField(t._1,typeToSparkType(t._2), true) )

    StructType(columnFields)

  }

  def getColumnNames(filename: String) = {
    scala.io.Source.fromFile(filename).getLines().toArray.map(_.trim)

  }


  def main(args: Array[String]): Unit = {



    val sparkConf = new SparkConf()
    val sc = new SparkContext(args(0), "programmativeschema", sparkConf)
    val sqlContext = new SQLContext(sc)

    val salesSchemaFromFile = getSchemaFromColumnType(args(2))

    val rawRDD = sc. textFile(args(1))


    sc.setLogLevel("WARN")

    val noHeaderSalesRDD = rawRDD.mapPartitionsWithIndex((sNo, pIntr) => {
      if (sNo == 0) pIntr.drop(1) else pIntr
    })

    val rowRDD = noHeaderSalesRDD.map(_.split(",")).map( p => {
      Row(p(0).trim.toInt,p(1).trim.toInt,p(2).trim.toInt,p(3).trim.toDouble)
    })

    val salesDF = sqlContext.createDataFrame(rowRDD, salesSchemaFromFile)

    salesDF.show()

  }
}
