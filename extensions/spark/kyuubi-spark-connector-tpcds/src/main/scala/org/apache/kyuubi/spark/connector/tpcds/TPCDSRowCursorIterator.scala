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

import com.google.common.collect.AbstractIterator
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.types._

import org.apache.kyuubi.tpcds.gen.RowCursor

private[tpcds] class TPCDSRowCursorIterator(generator: RowCursor, schema: StructType)
  extends AbstractIterator[InternalRow] {
  private val writer = new UnsafeRowWriter(generator.columnCount())
  // TPC-DS rows have at most 34 columns. Bit masks avoid allocating maps and ordinal
  // arrays for every short dimension scan while resolving the SQL types just once.
  require(schema.length == generator.columnCount() && schema.length <= 64)
  private val columnsByType = {
    val masks = new Array[Long](4)
    var i = 0
    while (i < schema.length) {
      val group = schema(i).dataType match {
        case LongType => 0
        case decimal: DecimalType if decimal.precision <= Decimal.MAX_LONG_DIGITS => 0
        case IntegerType => 1
        case DateType => 2
        case StringType | _: CharType | _: VarcharType => 3
        case unsupported =>
          throw new IllegalArgumentException(s"Unsupported TPC-DS type: $unsupported")
      }
      masks(group) |= 1L << i
      i += 1
    }
    masks
  }

  override protected def computeNext(): InternalRow = {
    if (!generator.next()) return endOfData()
    writer.reset()
    writer.zeroOutNullBytes()
    var remaining = columnsByType(0)
    while (remaining != 0) {
      val column = java.lang.Long.numberOfTrailingZeros(remaining)
      if (generator.isNull(column)) writer.setNullAt(column)
      else writer.write(column, generator.getLong(column))
      remaining &= remaining - 1
    }
    remaining = columnsByType(1)
    while (remaining != 0) {
      val column = java.lang.Long.numberOfTrailingZeros(remaining)
      if (generator.isNull(column)) writer.setNullAt(column)
      else writer.write(column, Math.toIntExact(generator.getLong(column)))
      remaining &= remaining - 1
    }
    remaining = columnsByType(2)
    while (remaining != 0) {
      val column = java.lang.Long.numberOfTrailingZeros(remaining)
      if (generator.isNull(column)) writer.setNullAt(column)
      else writer.write(column, Math.toIntExact(generator.getLong(column) - 2440588L))
      remaining &= remaining - 1
    }
    remaining = columnsByType(3)
    while (remaining != 0) {
      val column = java.lang.Long.numberOfTrailingZeros(remaining)
      if (generator.isNull(column)) writer.setNullAt(column)
      else writer.write(
        column,
        generator.getBytes(column),
        generator.getByteOffset(column),
        generator.getByteLength(column))
      remaining &= remaining - 1
    }
    writer.getRow
  }
}
