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

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StringType, StructField}
import org.slf4j.LoggerFactory

import org.apache.kyuubi.tpcds.gen.TpcdsTable

/**
 * Usage:
 * <p>
 * Run following command to generate 10GB data with new database `tpcds_sf10`.
 * {{{
 *   $SPARK_HOME/bin/spark-submit \
 *      --class org.apache.kyuubi.tpcds.DataGenerator \
 *      kyuubi-tpcds_*.jar \
 *      --db tpcds_sf10 --scaleFactor 10 --format parquet --parallel 20
 * }}}
 */
object DataGenerator {

  case class Config(
      db: String = "default",
      scaleFactor: Int = 1,
      format: String = "parquet",
      parallel: Option[Int] = None)

  private val logger = LoggerFactory.getLogger(this.getClass.getSimpleName.stripSuffix("$"))

  // Replicates Scala 2.12's `par.foreach` using stdlib concurrency, as Scala 2.13 requires
  // the external scala-parallel-collections module for `.par`.
  private def parForeach[T](items: Seq[T])(f: T => Unit): Unit = {
    Await.result(Future.traverse(items)(item => Future(f(item))), Duration.Inf)
  }

  def initTable(spark: SparkSession): Seq[TableGenerator] = {
    TpcdsTable.values().toSeq.map { table =>
      val fields = table.getColumns.toSeq.map { column =>
        val sqlType = column.getType
        val dataType = if (sqlType.startsWith("char") || sqlType.startsWith("varchar")) {
          StringType
        } else {
          DataType.fromDDL(sqlType)
        }
        StructField(column.getName, dataType, column.isNullable)
      }
      val partitions = table.getName match {
        case "inventory" => Seq("inv_date_sk")
        case name if name.endsWith("_sales") => Seq(fields.head.name)
        case name if name.endsWith("_returns") => Seq(fields.head.name)
        case _ => Nil
      }
      TableGenerator(table.getName, partitions, fields: _*)
    }
  }

  def run(config: Config): Unit = {
    val spark = SparkSession.builder()
      .appName(s"Kyuubi TPC-DS Generation - ${config.scaleFactor}GB")
      .enableHiveSupport()
      .getOrCreate()

    logger.info(s"Generating TPC-DS tables under database ${config.db}")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${config.db}")
    spark.sql(s"USE ${config.db}")
    spark.sql(s"DESC DATABASE ${config.db}").show()

    val tpcdsTables = initTable(spark)
    parForeach(tpcdsTables) { table =>
      table.setScaleFactor(config.scaleFactor)
      table.setFormat(config.format)
      config.parallel.foreach(table.setParallelism)
      spark.sparkContext.setJobDescription(table.toString)
      logger.info(s"Generating $table")
      table.create()
    }
  }

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("tpcds-data-generator") {
      head("Kyuubi TPC-DS Data Generator")
      opt[String]('d', "db")
        .action { (x, c) => c.copy(db = x) }
        .text("the database to write data")
      opt[Int]('s', "scaleFactor")
        .action { (x, c) => c.copy(scaleFactor = x) }
        .text("the scale factor of TPC-DS")
      opt[String]('f', "format")
        .action { (x, c) => c.copy(format = x) }
        .text("the format of table to store data")
      opt[Int]('p', "parallel")
        .action { (x, c) => c.copy(parallel = Some(x)) }
        .text("the parallelism of Spark job")
      help('h', "help")
        .text("prints this usage text")
    }

    parser.parse(args, Config()) match {
      case Some(config) => run(config)
      case None => sys.exit(1)
    }
  }
}
