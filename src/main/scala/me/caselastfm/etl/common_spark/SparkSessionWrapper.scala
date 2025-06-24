package me.caselastfm.etl.common_spark

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {
    
  lazy val spark: SparkSession = {
      SparkSession
        .builder()
        .appName("LastFmStats")
        .config("spark.master", ConfigParser.sparkMaster)
        .config("spark.driver.host", ConfigParser.sparkDriverHost)
        .config("spark.executor.memory", ConfigParser.sparkExecutorMemory)
        .config("spark.driver.memory", ConfigParser.sparkDriverMemory)
        .config("spark.hadoop.validateOutputSpecs", ConfigParser.sparkOutputExists)
        .getOrCreate()
    }

}
