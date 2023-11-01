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

import java.util.{List => JList}

import io.trino.tpcds.generator.TimeDimGeneratorColumn._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

class TimeDimRow(
    nullBitMap: Long,
    private val tTimeSk: Long,
    private val tTimeId: String,
    private val tTime: Int,
    private val tHour: Int,
    private val tMinute: Int,
    private val tSecond: Int,
    private val tAmPm: String,
    private val tShift: String,
    private val tSubShift: String,
    private val tMealTime: String) extends TableRowWithNulls(nullBitMap, T_TIME_SK)
  with KyuubiTableRowWithNulls {

  override val nullBitMapInternal = nullBitMap
  override val firstColumnInternal = T_TIME_SK

  def getTTimeSk: Long = tTimeSk
  def getTTimeId: String = tTimeId
  def getTTime: Int = tTime
  def getTHour: Int = tHour
  def getTMinute: Int = tMinute
  def getTSecond: Int = tSecond
  def getTAmPm: String = tAmPm
  def getTShift: String = tShift
  def getTSubShift: String = tSubShift
  def getTMealTime: String = tMealTime

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow =
    new GenericInternalRow(
      Array(
        getLongOrNull(tTimeSk, T_TIME_SK),
        getStringOrNullInternal(tTimeId, T_TIME_ID),
        getIntOrNull(tTime, T_TIME),
        getIntOrNull(tHour, T_HOUR),
        getIntOrNull(tMinute, T_MINUTE),
        getIntOrNull(tSecond, T_SECOND),
        getStringOrNullInternal(tAmPm, T_AM_PM),
        getStringOrNullInternal(tShift, T_SHIFT),
        getStringOrNullInternal(tSubShift, T_SUB_SHIFT),
        getStringOrNullInternal(tMealTime, T_MEAL_TIME)))
}
