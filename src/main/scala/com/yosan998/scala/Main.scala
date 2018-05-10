package com.yosan998.scala

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Main extends App with LazyLogging {

  this.logger.info("Init process ExampleSparkJob")

  val conf = new SparkConf().setMaster("local").setAppName("Main")
  val sc =  new SparkContext(conf)

  val defaultConfig = ConfigFactory.parseResources("application.conf")

  val builder = SparkSession.builder().config(conf).master("local").getOrCreate()

}
