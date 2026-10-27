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

package org.apache.kyuubi.spark.connector.tpch

import java.math.RoundingMode
import java.util.OptionalLong

import com.google.common.math.LongMath
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types._

import org.apache.kyuubi.tpch.gen.{TpchGenerator, TpchTable}

case class TPCHTableChuck(table: String, scale: Double, parallelism: Int, index: Int)
  extends InputPartition

class TPCHBatchScan(
    @transient table: TpchTable,
    scale: Double,
    schema: StructType,
    readConf: TPCHReadConf) extends ScanBuilder
  with SupportsReportStatistics with Batch with Serializable {

  private val _sizeInBytes: Long = TPCHStatisticsUtils.sizeInBytes(table, scale)

  private val _numRows: Long = TPCHStatisticsUtils.numRows(table, scale)

  require(readConf.maxPartitionBytes > 0, "maxPartitionBytes must be positive")

  private val parallelism: Int =
    if (table.equals(TpchTable.NATION) || table.equals(TpchTable.REGION)) 1
    else math.max(
      SparkSession.active.sparkContext.defaultParallelism,
      Math.toIntExact(LongMath.divide(
        _sizeInBytes,
        readConf.maxPartitionBytes,
        RoundingMode.CEILING)))

  override def build: Scan = this

  override def toBatch: Batch = this

  override def description: String =
    s"Scan TPC-H ${TPCHSchemaUtils.dbName(scale)}.${table.getTableName}, " +
      s"count: ${_numRows}, parallelism: $parallelism"

  override def readSchema: StructType = schema

  override def planInputPartitions: Array[InputPartition] =
    (1 to parallelism).map { i =>
      TPCHTableChuck(table.getTableName, scale, parallelism, i)
    }.toArray

  def createReaderFactory: PartitionReaderFactory = (partition: InputPartition) => {
    val chuck = partition.asInstanceOf[TPCHTableChuck]
    new TPCHPartitionReader(chuck.table, chuck.scale, chuck.parallelism, chuck.index, schema)
  }

  override def estimateStatistics: Statistics = new Statistics {
    override def sizeInBytes: OptionalLong = OptionalLong.of(_sizeInBytes)
    override def numRows: OptionalLong = OptionalLong.of(_numRows)
  }

}

class TPCHPartitionReader(
    table: String,
    scale: Double,
    parallelism: Int,
    index: Int,
    schema: StructType) extends PartitionReader[InternalRow] {

  private val cursor = TpchGenerator.partition(TpchTable.getTable(table), scale, parallelism, index)
  private val writer = new UnsafeRowWriter(cursor.columnCount())
  // Resolve the schema once. Each loop has one primitive write operation instead of a
  // data-type match for every value; string ordinals retain their original order.
  private val columnsByType = schema.fields.zipWithIndex.groupBy { case (field, _) =>
    field.dataType match {
      case LongType => LongType
      case IntegerType | DateType => IntegerType
      case DoubleType => DoubleType
      case StringType | _: CharType | _: VarcharType => StringType
      case unsupported =>
        throw new IllegalArgumentException(s"Unsupported TPC-H type: $unsupported")
    }
  }.map { case (dataType, columns) => dataType -> columns.map(_._2) }
  private val longColumns = columnsByType.getOrElse(LongType, Array.emptyIntArray)
  private val integerColumns = columnsByType.getOrElse(IntegerType, Array.emptyIntArray)
  private val doubleColumns = columnsByType.getOrElse(DoubleType, Array.emptyIntArray)
  private val stringColumns = columnsByType.getOrElse(StringType, Array.emptyIntArray)

  override def next(): Boolean = {
    if (!cursor.next()) return false
    writer.reset()
    writer.zeroOutNullBytes()
    var i = 0
    while (i < longColumns.length) {
      val column = longColumns(i)
      writer.write(column, cursor.getLong(column))
      i += 1
    }
    i = 0
    while (i < integerColumns.length) {
      val column = integerColumns(i)
      writer.write(column, Math.toIntExact(cursor.getLong(column)))
      i += 1
    }
    i = 0
    while (i < doubleColumns.length) {
      val column = doubleColumns(i)
      writer.write(column, cursor.getDouble(column))
      i += 1
    }
    i = 0
    while (i < stringColumns.length) {
      val column = stringColumns(i)
      writer.write(
        column,
        cursor.getBytes(column),
        cursor.getByteOffset(column),
        cursor.getByteLength(column))
      i += 1
    }
    true
  }

  override def get(): InternalRow = writer.getRow

  override def close(): Unit = {}
}
