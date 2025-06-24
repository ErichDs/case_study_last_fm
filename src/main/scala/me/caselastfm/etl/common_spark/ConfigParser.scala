package me.caselastfm.etl.common_spark

import com.typesafe.config.{Config, ConfigFactory}

object ConfigParser {

  private lazy val conf: Config = ConfigFactory.load()

  lazy val sparkMaster: String = conf.getString("sparkMaster")
  lazy val sparkDriverHost: String = conf.getString("sparkDriverHost")
  lazy val sparkExecutorMemory: String = conf.getString("sparkExecutorMemory")
  lazy val sparkDriverMemory: String = conf.getString("sparkDriverMemory")
  lazy val sparkOutputExists: String = conf.getString("sparkOutputExists")

  lazy val dataSource: String = conf.getString("dataSource")
  lazy val outputFilePath: String = conf.getString("outputFilePath")
  lazy val outputFileName: String = conf.getString("outputFileName")

}
