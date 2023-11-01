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

import io.trino.tpcds.generator.HouseholdDemographicsGeneratorColumn._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

class HouseholdDemographicsRow(
    nullBitMap: Long,
    private val hdDemoSk: Long,
    private val hdIncomeBandId: Long,
    private val hdBuyPotential: String,
    private val hdDepCount: Int,
    private val hdVehicleCount: Int) extends TableRowWithNulls(nullBitMap, HD_DEMO_SK)
  with KyuubiTableRowWithNulls {

  override val nullBitMapInternal = nullBitMap
  override val firstColumnInternal = HD_DEMO_SK

  def getHdDemoSk: Long = hdDemoSk
  def getHdIncomeBandId: Long = hdIncomeBandId
  def getHdBuyPotential: String = hdBuyPotential
  def getHdDepCount: Int = hdDepCount
  def getHdVehicleCount: Int = hdVehicleCount

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow =
    new GenericInternalRow(
      Array(
        getLongOrNull(hdDemoSk, HD_DEMO_SK),
        getLongOrNull(hdIncomeBandId, HD_INCOME_BAND_ID),
        getStringOrNullInternal(hdBuyPotential, HD_BUY_POTENTIAL),
        getIntOrNull(hdDepCount, HD_DEP_COUNT),
        getIntOrNull(hdVehicleCount, HD_VEHICLE_COUNT)))
}
