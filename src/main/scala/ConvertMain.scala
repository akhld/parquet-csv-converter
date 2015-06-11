import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by akhld on 11/6/15.
 */

object ConvertMain {

  def main(args: Array[String]): Unit ={

    if (args.length != 2) {
      println("---------------++++++++++++++---------------")
      println("Usage: sbt 'run input.parquet output.csv'")
      println("---------------++++++++++++++---------------")
      System.exit(1)
    }

    val sConf = new SparkConf().setAppName("Parquet-Csv-Converter").setMaster("local[4]")
    val sc = new SparkContext(sConf)
    val sqc = new SQLContext(sc)
    sqc.setConf("spark.sql.parquet.binaryAsString","true")

    val d = sqc.parquetFile(args(0))
    d.toJavaRDD.saveAsTextFile(args(1))


  }

}
