package com.yosan998.scala

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

object Main extends App with LazyLogging {

  this.logger.info("Init process ExampleSparkJob")

  val conf = new SparkConf().setMaster("local").setAppName("Main")
  val sc =  new SparkContext(conf)

  val defaultConfig = ConfigFactory.parseResources("application.conf")

  val builder = SparkSession.builder().config(conf).master("local").getOrCreate()

  val budgetDF = builder
    .read
    .format("csv")
    .option("header", "true")
    .load(defaultConfig.getString("ExampleSparkJob.input.budgetInputFile"))

  val gdpDF = builder
    .read
    .format("csv")
    .option("header", "true")
    .load(defaultConfig.getString("ExampleSparkJob.input.gdpInputFile"))

  this.logger.info("Filtering gdp by country code = USA and renaming the column and dropping column Country Name")
  val gdpUSADF = gdpDF
    .filter(col("Country Code") === "USA")
    .withColumnRenamed("Country Code", "Country_Code")
    .drop(col("Country Name"))


  this.logger.info("Filtering budget by Year < 2017 and creating column Value in millions")
  val budgetFilteredDF = budgetDF
    .filter("Year < 2017")
    .withColumn("ValueMillions", col("Value") * 1000000)

  this.logger.info("Filtering budget by Department of Education and renaming the column and dropping column Value")
  val budgetFiltered2 = budgetFilteredDF
    .filter(col("Name") === "Department of Education")
    .withColumnRenamed("ValueMillions", "budgetValue")
    .drop("Value")

  this.logger.info("Joining budget with gdp by column Year and dropping the column to avoid duplicates")
  val gdpJoinBudgetEducationDF = budgetFiltered2.as("df1")
    .join(gdpUSADF.as("df2"), col("df1.Year") === col("df2.Year"), "left")
    .drop(col("df2.Year"))

  this.logger.info("Creating new column Ratio")
  val result = gdpJoinBudgetEducationDF
    .withColumn("Ratio", col("budgetValue")/col("value"))

  this.logger.info("Writting result1")
  result
    .write
    .format("com.databricks.spark.csv")
    .mode("overwrite")
    .save(defaultConfig.getString("ExampleSparkJob.output.result1"))


}
