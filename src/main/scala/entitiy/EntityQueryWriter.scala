package org.example.application
package entitiy

import entitiy.EntityQueryWriter.write

import com.typesafe.config.Config
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object EntityQueryWriter {
  def write(batchDf: DataFrame, batchId: Long, config: Config): Unit = {

    println(batchId)
    batchDf.write
      .format("org.opensearch.spark.sql")
      .option("opensearch.resource", config.getString("opensearch.index"))
      .option("opensearch.nodes", config.getString("opensearch.host"))
      .option("opensearch.port", config.getString("opensearch.port"))
      .mode("append")
      .save()
  }
}

class EntityQueryWriter(override val dataFrame: DataFrame, val config: Config)
  extends Query(dataFrame) {

  private val checkpointLocation: String = config.getString("checkpointLocation")

  val spark: SparkSession = dataFrame.sparkSession

  val query = dataFrame.select("data.*")
    .writeStream
    .option("checkpointLocation", s"$checkpointLocation/$name")
    .foreachBatch((batchDF: DataFrame, batchId: Long) => write(batchDF, batchId, config))
    .outputMode(OutputMode.Append())
    .trigger(Trigger.ProcessingTime("1 second"))
    .queryName(name)

  override def start(): StreamingQuery = query.start()
}
