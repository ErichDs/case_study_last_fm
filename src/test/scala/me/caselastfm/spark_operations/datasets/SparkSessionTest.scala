package me.caselastfm.spark_operations.datasets

import org.apache.spark.sql._

trait SparkSessionTest {

    val sparkSession: SparkSession = {
      SparkSession
        .builder()
        .master("local")
        .appName("test-spark-app")
        .getOrCreate()
    }
}
