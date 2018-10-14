package hacked.work

import org.apache.spark.sql.SparkSession

/**
 * Created by akhld on 11/6/15.
 */

object ParquetCSVConvertMain {

  def main(args: Array[String]): Unit ={

    if (args.length != 2) {
      println("---------------++++++++++++++---------------")
      println("Usage: sbt 'run input.parquet output.csv'")
      println("---------------++++++++++++++---------------")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("ParquetCsvConverter")
      .getOrCreate()

    import spark.implicits._

    spark.sqlContext
      .read.parquet(args(0))
      .map(_.mkString(","))
      .write.text(args(1))

  }

}
