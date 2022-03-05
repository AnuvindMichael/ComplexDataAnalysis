package ComplexDataAnalysis

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import sys.process._
object SparkObj {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    println()
    println("====================READ RAW DATA==================")
    println()

    val rawdf = spark.read.format("json").option("multiLine","true").load("file:///D:/Practice/complex.json")
    rawdf.show()
    rawdf.printSchema()

    println()
    println("====================Flatten DATA==================")
    println()

    val flatendf = rawdf.select(col("nationality"),col("seed"),col("version"),col("results"))
    flatendf.show()
    flatendf.printSchema()

    val arradf = flatendf.withColumn("results",explode(col("results")))

    arradf.show()
    arradf.printSchema()

      val finaldf = arradf.select(
                    col("nationality"),
                    col("seed"),
                    col("version"),
                    col("results.user.SSN"),
                    col("results.user.cell"),
                    col("results.user.dob"),
                    col("results.user.email"),
                    col("results.user.gender"),
                    col("results.user.location.city"),
                    col("results.user.location.state"),
                    col("results.user.location.street"),
                    col("results.user.location.zip"),
                    col("results.user.md5"),
                    col("results.user.name.first"),
                    col("results.user.name.last"),
                    col("results.user.name.title"),
                    col("results.user.password"),
                    col("results.user.phone"),
                    col("results.user.picture.large"),
                    col("results.user.picture.medium"),
                    col("results.user.picture.thumbnail"),
                    col("results.user.registered"),
                    col("results.user.salt"),
                    col("results.user.sha1"),
                    col("results.user.sha256"),
                    col("results.user.username")

                  )
    finaldf.show()
    finaldf.printSchema()

    println()
    println("====================Reverting  DATA==================")
    println()

    val df = finaldf.groupBy("nationality","seed","version")
      .agg(
        collect_list(
          struct(

            struct(
              col("SSN"),
              col("cell"),
              col("dob"),
              col("email"),
              col("gender"),
              struct(
                col("city"),
                col("state"),
                col("street"),
                col("zip")

              ).alias("location"),
              col("md5"),
              struct(
                col("first"),
                col("last"),
                col("title")
              ).alias("name"),
              col("password"),
              col("phone"),
              struct(
                col("large"),
                col("medium"),
                col("thumbnail")
              ).alias("picture"),
              col("registered"),
              col("salt"),
              col("sha1"),
              col("sha256"),
              col("username")
            ).alias("user")
          )

        ).alias("results"))

    df.show()
    df.printSchema()








  }

}
