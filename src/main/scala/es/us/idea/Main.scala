package es.us.idea

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/DQDMN.test")
      .appName("DMN DQ Sample").getOrCreate()

    import es.us.idea.dmn4spark.implicits._

    val df = spark.read.option("header", "true").csv("input/filtered-aws-ec2-data-split.csv")
      .dmn.loadFromLocalPath("models/dmn4dq.dmn")

    MongoSpark.save(df)
  }

}
