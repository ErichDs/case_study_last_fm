package me.caselastfm.etl

import me.caselastfm.etl.common_spark.{ConfigParser, IOUtils, SparkSessionWrapper}
import me.caselastfm.etl.spark_operations.datasets.LastFMSessionsOps
import org.apache.spark.sql.SQLContext

object etlApp extends SparkSessionWrapper {

  def main(args: Array[String]): Unit = {

    implicit val sqlc: SQLContext = spark.sqlContext

      val sparkOp: LastFMSessionsOps = new LastFMSessionsOps

      IOUtils.writeDF(sparkOp.definition, ConfigParser.outputFileName, ConfigParser.outputFilePath, "\t")
  }

}
