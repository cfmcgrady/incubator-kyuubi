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

import java.util.{ArrayList => JArrayList, Iterator => JIterator, List => JList}

import com.google.common.collect.AbstractIterator
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._

import org.apache.kyuubi.tpcds.gen.{TpcdsGenerator, TpcdsTable}

private[tpcds] object TPCDSRowIterator {
  def typed(
      table: TpcdsTable,
      scale: Double,
      parallelism: Int,
      partition: Int,
      schema: StructType): JIterator[Row] = {
    val generator = TpcdsGenerator.partition(table, scale, parallelism, partition)
    require(schema.length == generator.columnCount(), "TPC-DS schema length mismatch")
    val types = schema.fields.map(_.dataType)
    new AbstractIterator[Row] {
      override protected def computeNext(): Row = {
        if (!generator.next()) return endOfData()
        // Spark's external Row interface owns its values; these rows may outlive the cursor.
        val values = new Array[Any](types.length)
        var i = 0
        while (i < values.length) {
          if (!generator.isNull(i)) {
            values(i) = types(i) match {
              case LongType => generator.getLong(i)
              case IntegerType => Math.toIntExact(generator.getLong(i))
              case DateType => java.sql.Date.valueOf(
                  java.time.LocalDate.ofEpochDay(generator.getLong(i) - 2440588L))
              case decimal: DecimalType =>
                java.math.BigDecimal.valueOf(generator.getLong(i), decimal.scale)
              case StringType | _: CharType | _: VarcharType => generator.getString(i)
              case unsupported =>
                throw new IllegalArgumentException(s"Unsupported TPC-DS type: $unsupported")
            }
          }
          i += 1
        }
        new GenericRow(values)
      }
    }
  }

  def apply(
      table: TpcdsTable,
      scale: Double,
      parallelism: Int,
      partition: Int): JIterator[JList[String]] = {
    val generator = TpcdsGenerator.partition(table, scale, parallelism, partition)
    new AbstractIterator[JList[String]] {
      override protected def computeNext(): JList[String] = {
        if (!generator.next()) return endOfData()
        val row = new JArrayList[String](generator.columnCount())
        var i = 0
        while (i < generator.columnCount()) {
          row.add(generator.getString(i))
          i += 1
        }
        row
      }
    }
  }
}
