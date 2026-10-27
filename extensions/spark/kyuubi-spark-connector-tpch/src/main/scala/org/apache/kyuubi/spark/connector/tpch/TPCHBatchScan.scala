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
import org.apache.spark.unsafe.types.UTF8String

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
  private val types = schema.fields.map(_.dataType)

  override def next(): Boolean = {
    if (!cursor.next()) return false
    writer.reset()
    writer.zeroOutNullBytes()
    var i = 0
    while (i < cursor.columnCount()) {
      types(i) match {
        case LongType => writer.write(i, cursor.getLong(i))
        case IntegerType | DateType => writer.write(i, Math.toIntExact(cursor.getLong(i)))
        case DoubleType => writer.write(i, cursor.getDouble(i))
        case StringType | _: CharType | _: VarcharType =>
          writer.write(i, UTF8String.fromString(cursor.getString(i)))
        case unsupported =>
          throw new IllegalArgumentException(s"Unsupported TPC-H type: $unsupported")
      }
      i += 1
    }
    true
  }

  override def get(): InternalRow = writer.getRow

  override def close(): Unit = {}
}
