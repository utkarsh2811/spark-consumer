package org.example.application

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

abstract class Query(val dataFrame: DataFrame) {

  def name = this.getClass.getName

  def start(): StreamingQuery
}

