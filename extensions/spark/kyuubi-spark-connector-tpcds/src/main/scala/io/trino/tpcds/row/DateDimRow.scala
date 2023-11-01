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

import io.trino.tpcds.generator.DateDimGeneratorColumn._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

class DateDimRow(
    nullBitMap: Long,
    private val dDateSk: Long,
    private val dDateId: String,
    private val dMonthSeq: Int,
    private val dWeekSeq: Int,
    private val dQuarterSeq: Int,
    private val dYear: Int,
    private val dDow: Int,
    private val dMoy: Int,
    private val dDom: Int,
    private val dQoy: Int,
    private val dFyYear: Int,
    private val dFyQuarterSeq: Int,
    private val dFyWeekSeq: Int,
    private val dDayName: String,
    private val dHoliday: Boolean,
    private val dWeekend: Boolean,
    private val dFollowingHoliday: Boolean,
    private val dFirstDom: Int,
    private val dLastDom: Int,
    private val dSameDayLy: Int,
    private val dSameDayLq: Int,
    private val dCurrentDay: Boolean,
    private val dCurrentWeek: Boolean,
    private val dCurrentMonth: Boolean,
    private val dCurrentQuarter: Boolean,
    private val dCurrentYear: Boolean) extends TableRowWithNulls(nullBitMap, D_DATE_SK)
  with KyuubiTableRowWithNulls {

  override val nullBitMapInternal = nullBitMap
  override val firstColumnInternal = D_DATE_SK

  def getdDateSk: Long = dDateSk

  def getdDateId: String = dDateId

  def getdMonthSeq: Int = dMonthSeq

  def getdWeekSeq: Int = dWeekSeq

  def getdQuarterSeq: Int = dQuarterSeq

  def getdYear: Int = dYear

  def getdDow: Int = dDow

  def getdMoy: Int = dMoy

  def getdDom: Int = dDom

  def getdQoy: Int = dQoy

  def getdFyYear: Int = dFyYear

  def getdFyQuarterSeq: Int = dFyQuarterSeq

  def getdFyWeekSeq: Int = dFyWeekSeq

  def getdDayName: String = dDayName

  def isdHoliday: Boolean = dHoliday

  def isdWeekend: Boolean = dWeekend

  def isdFollowingHoliday: Boolean = dFollowingHoliday

  def getdFirstDom: Int = dFirstDom

  def getdLastDom: Int = dLastDom

  def getdSameDayLy: Int = dSameDayLy

  def getdSameDayLq: Int = dSameDayLq

  def isdCurrentDay: Boolean = dCurrentDay

  def isdCurrentWeek: Boolean = dCurrentWeek

  def isdCurrentMonth: Boolean = dCurrentMonth

  def isdCurrentQuarter: Boolean = dCurrentQuarter

  def isdCurrentYear: Boolean = dCurrentYear

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow =
    new GenericInternalRow(
      Array(
        getLongOrNull(dDateSk, D_DATE_SK),
        getStringOrNullInternal(dDateId, D_DATE_ID),
        getDateOrNullFromJulianDays(dDateSk, D_DATE_SK),
        getIntOrNull(dMonthSeq, D_MONTH_SEQ),
        getIntOrNull(dWeekSeq, D_WEEK_SEQ),
        getIntOrNull(dQuarterSeq, D_QUARTER_SEQ),
        getIntOrNull(dYear, D_YEAR),
        getIntOrNull(dDow, D_DOW),
        getIntOrNull(dMoy, D_MOY),
        getIntOrNull(dDom, D_DOM),
        getIntOrNull(dQoy, D_QOY),
        getIntOrNull(dFyYear, D_FY_YEAR),
        getIntOrNull(dFyQuarterSeq, D_FY_QUARTER_SEQ),
        getIntOrNull(dFyWeekSeq, D_FY_WEEK_SEQ),
        getStringOrNullInternal(dDayName, D_DAY_NAME),
        getStringOrNullInternal(
          java.lang.String.format("%4dQ%d", dYear.asInstanceOf[Object], dQoy.asInstanceOf[Object]),
          D_QUARTER_NAME),
        getStringOrNullInternal(dHoliday, D_HOLIDAY),
        getStringOrNullInternal(dWeekend, D_WEEKEND),
        getStringOrNullInternal(dFollowingHoliday, D_FOLLOWING_HOLIDAY),
        getIntOrNull(dFirstDom, D_FIRST_DOM),
        getIntOrNull(dLastDom, D_LAST_DOM),
        getIntOrNull(dSameDayLy, D_SAME_DAY_LY),
        getIntOrNull(dSameDayLq, D_SAME_DAY_LQ),
        getStringOrNullInternal(dCurrentDay, D_CURRENT_DAY),
        getStringOrNullInternal(dCurrentWeek, D_CURRENT_WEEK),
        getStringOrNullInternal(dCurrentMonth, D_CURRENT_MONTH),
        getStringOrNullInternal(dCurrentQuarter, D_CURRENT_QUARTER),
        getStringOrNullInternal(dCurrentYear, D_CURRENT_YEAR)))
}
