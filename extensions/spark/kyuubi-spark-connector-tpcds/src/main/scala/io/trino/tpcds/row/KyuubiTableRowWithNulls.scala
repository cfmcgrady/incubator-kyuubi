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

package io.trino.tpcds.row

import io.trino.tpcds.Options
import io.trino.tpcds.`type`.{Decimal => TPCDSDecimal}
import io.trino.tpcds.generator.GeneratorColumn
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, RebaseDateTime}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

trait KyuubiTableRowWithNulls { self: TableRowWithNulls =>

  def isNullInternal(column: GeneratorColumn): Boolean = {
    val kBitMask = 1L << (column.getGlobalColumnNumber - firstColumnInternal.getGlobalColumnNumber)
    (nullBitMapInternal & kBitMask) != 0
  }

  def getLongOrNull(value: Long, column: GeneratorColumn): Any = {
    if (isNullInternal(column) || (value == -1L)) null else value
  }
//  protected <T> String getStringOrNullForKey(long value, GeneratorColumn column)
//    {
//    return (isNull(column) || value == -1) ? null : Long.toString(value);
//    }

  def getLongOrNull(value: Int, column: GeneratorColumn): Any = {
    if (isNullInternal(column) || (value == -1)) null else value.toLong
  }

  def getDecimalOrNull(
      value: TPCDSDecimal,
      column: GeneratorColumn,
      precision: Int,
      scale: Int): Decimal = {
    if (isNullInternal(column)) null else Decimal(value.getNumber, precision, scale)
  }

  def getDecimalOrNull(
      value: Int,
      column: GeneratorColumn,
      precision: Int,
      scale: Int): Decimal = {
    if (isNullInternal(column)) null
    else {
      val decimal = Decimal(value)
      decimal.changePrecision(precision, scale)
      decimal
    }
  }

  def getIntOrNull(value: Long, column: GeneratorColumn): Any = {
    if (isNullInternal(column)) null else value.toInt
  }

  def getIntOrNull(value: Int, column: GeneratorColumn): Any = {
    if (isNullInternal(column)) null else value
  }

  def getIntOrNullForKey(value: Long, column: GeneratorColumn): Any = {
    if (isNullInternal(column) || (value == -1L)) null else value.toInt
  }

  def getIntOrNullForKey(value: Int, column: GeneratorColumn): Any = {
    if (isNullInternal(column) || (value == -1)) null else value
  }

  def getStringOrNullInternal(value: String, column: GeneratorColumn): UTF8String = {
    if (isNullInternal(column) || value == null || value == Options.DEFAULT_NULL_STRING) {
      null
    } else UTF8String.fromString(value)
  }

  def getStringOrNullInternal(value: Boolean, column: GeneratorColumn): UTF8String = {
    if (isNullInternal(column)) {
      null
    } else {
      if (value) KyuubiTableRowWithNulls.TRUE_STRING else KyuubiTableRowWithNulls.FALSE_STRING
    }
  }

  def getDateOrNullFromJulianDays(value: Long, column: GeneratorColumn): Any = {
    if (isNullInternal(column) || value < 0) {
      null
    } else {
      RebaseDateTime.rebaseJulianToGregorianDays(value.toInt) - DateTimeUtils.JULIAN_DAY_OF_EPOCH
    }
  }

  def nullBitMapInternal: Long
  def firstColumnInternal: GeneratorColumn
  def internalRow: InternalRow
}

object KyuubiTableRowWithNulls {
  private val TRUE_STRING = UTF8String.fromString("Y")
  private val FALSE_STRING = UTF8String.fromString("N")
}
