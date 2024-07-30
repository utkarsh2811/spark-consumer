package org.example.application

import entitiy.EntityQueryWriter
import source.KafkaSource
import source.KafkaSource.nestedSchema

import com.typesafe.config.ConfigFactory
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Application extends App {
  val config = ConfigFactory.load()
  val sparkConf = new SparkConf()

  val logger = LogManager.getLogger(this.getClass.getName)

  sparkConf.set("spark.opensearch.nodes", config.getString("opensearch.host"))
  sparkConf.set("spark.opensearch.port", config.getString("opensearch.port"))
  sparkConf.set("spark.opensearch.nodes.wan.only", "true")
  sparkConf.set("spark.opensearch.ssl", "false")

  import org.apache.log4j.{Level, Logger}

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val dataSource = new KafkaSource(spark, config.getString("kafka.dataTopic"))
  val parsedDf = dataSource.data
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), nestedSchema).as("data"))

  val queries = List(
    new EntityQueryWriter(parsedDf, config)
  )

  queries.foreach(_.start())

  logger.info("Queries started")

  spark.streams.awaitAnyTermination()
}
