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

package org.apache.kyuubi.spark.connector.tpcds

import java.text.DecimalFormat

import org.apache.kyuubi.tpcds.gen.{TpcdsTable => Table}

object TPCDSSchemaUtils {

  val TINY_SCALE = "0.01"

  val SCALES: Array[String] =
    Array("0", TINY_SCALE, "1", "10", "100", "300", "1000", "3000", "10000", "30000", "100000")

  val TINY_DB_NAME = "tiny"

  val DATABASES: Array[String] = SCALES.map {
    case TINY_SCALE => TINY_DB_NAME
    case scale => s"sf$scale"
  }

  def normalize(scale: Double): String = new DecimalFormat("#.##").format(scale)

  def scale(dbName: String): Double = SCALES(DATABASES.indexOf(dbName)).toDouble

  def dbName(scale: Double): String = DATABASES(SCALES.indexOf(normalize(scale)))

  val BASE_TABLES: Array[Table] = Table.values()

  def reviseColumnName(col: Table.Column, useTableSchema_2_6: Boolean): String =
    if (col.getName == "c_last_review_date_sk" && !useTableSchema_2_6) {
      "c_last_review_date"
    } else {
      col.getName
    }

  def tablePartitionColumnNames(table: Table, useTableSchema_2_6: Boolean): Array[String] =
    table.getName match {
      case "inventory" => Array("inv_date_sk")
      case name if name.endsWith("_sales") || name.endsWith("_returns") =>
        Array(table.column(0).getName)
      case _ => Array.empty
    }
}
