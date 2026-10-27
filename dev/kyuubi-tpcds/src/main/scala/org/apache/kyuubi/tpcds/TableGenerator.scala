/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.tpcds

import scala.collection.JavaConverters._

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}

import org.apache.kyuubi.tpcds.gen.TpcdsTable

case class TableGenerator(
    name: String,
    partitionCols: Seq[String],
    fields: StructField*) {

  private val schema: StructType = StructType(fields)

  private var scaleFactor: Int = 1
  def setScaleFactor(scale: Int): Unit = this.scaleFactor = scale

  private var _parallelism: Option[Int] = None
  private def parallelism: Int = _parallelism.getOrElse(scaleFactor * 2)
  def setParallelism(parallel: Int): Unit = this._parallelism = Some(parallel max 2)

  private val ss: SparkSession = SparkSession.active
  private var _format: Option[String] = None
  private def format: String = _format.getOrElse(ss.conf.get("spark.sql.sources.default"))
  def setFormat(format: String): Unit = this._format = Some(format)

  private def radix: Int = (scaleFactor / 100) max 5 min parallelism

  private[tpcds] def toDF: DataFrame = {
    val rowRDD = ss.sparkContext.parallelize(1 to parallelism, parallelism).flatMap { i =>
      TPCDSRowIterator.typed(TpcdsTable.getTable(name), scaleFactor, parallelism, i, schema).asScala
    }

    ss.createDataFrame(rowRDD, schema)
  }

  def create(): Unit = {
    val data =
      if (partitionCols.isEmpty) {
        toDF.repartition(radix)
      } else {
        toDF.persist()
      }

    val tempViewName = s"${name}_view"

    data.createOrReplaceTempView(tempViewName)

    val writer =
      if (partitionCols.nonEmpty) {
        val query =
          s"""
             |(SELECT
             | ${fields.map(_.name).mkString(", ")}
             |FROM
             | $tempViewName WHERE ${partitionCols.head} IS NOT NULL
             |DISTRIBUTE BY ${partitionCols.head})
             |UNION ALL
             |(SELECT
             | ${fields.map(_.name).mkString(", ")}
             |FROM
             | $tempViewName
             |WHERE ${partitionCols.head} IS NULL
             |DISTRIBUTE BY CAST(RAND() * $radix AS INT))
             |""".stripMargin

        ss.sql(query).write.partitionBy(partitionCols: _*)
      } else {
        data.write
      }
    ss.sql(s"DROP TABLE IF EXISTS $name")
    writer.format(format).saveAsTable(name)
    data.unpersist()
    ss.sqlContext.dropTempTable(tempViewName)
  }

  override def toString: String = {
    s"""
       |Table: $name
       |Format: $format
       |ScaleFactor: ${scaleFactor}GB
       |Schema:${schema.sql}
       |Partitions: ${partitionCols.mkString("[", ", ", "]")}
       |""".stripMargin
  }
}
