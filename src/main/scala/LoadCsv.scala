import org.apache.spark.{SparkConf, SparkContext}


object LoadCsv {

  def main(args: Array[String]): Unit = {

    val master = "local"
    val input = "D:\\Spark\\Usecase\\SparkLoad\\salescsv.csv"
    // val output = args(2)

    val sparkConf = new SparkConf()
    val sc : SparkContext = new SparkContext(master , "csvfile", sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    sc.setLogLevel("WARN")

    val opt = Map("header" -> "true", "InferSchema" -> "true")

    val info = sqlContext.read
                          .format("csv")
                          .options(opt)
                          .load(input)

    info.printSchema()
    info.show()

    //info.write.format("csv").options(opt).save(output)
  }

}
