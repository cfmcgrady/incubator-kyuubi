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

import io.trino.tpcds.generator.CustomerDemographicsGeneratorColumn._
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.InternalRow

import java.util.{List => JList}

class CustomerDemographicsRow(
                               nullBitMap: Long,
                               private val cdDemoSk: Long,
                               private val cdGender: String,
                               private val cdMaritalStatus: String,
                               private val cdEducationStatus: String,
                               private val cdPurchaseEstimate: Int,
                               private val cdCreditRating: String,
                               private val cdDepCount: Int,
                               private val cdDepEmployedCount: Int,
                               private val cdDepCollegeCount: Int

                               ) extends TableRowWithNulls(nullBitMap, CD_DEMO_SK)
  with KyuubiTableRowWithNulls {

    override val nullBitMapInternal = nullBitMap
    override val firstColumnInternal = CD_DEMO_SK

  def getCdDemoSk: Long = cdDemoSk
  def getCdGender: String = cdGender
  def getCdMaritalStatus: String = cdMaritalStatus
  def getCdEducationStatus: String = cdEducationStatus
  def getCdPurchaseEstimate: Int = cdPurchaseEstimate
  def getCdCreditRating: String = cdCreditRating
  def getCdDepCount: Int = cdDepCount
  def getCdDepEmployedCount: Int = cdDepEmployedCount
  def getCdDepCollegeCount: Int = cdDepCollegeCount

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow =
    new GenericInternalRow(
      Array(
        getLongOrNull(cdDemoSk, CD_DEMO_SK),
        getStringOrNullInternal(cdGender, CD_GENDER),
        getStringOrNullInternal(cdMaritalStatus, CD_MARITAL_STATUS),
        getStringOrNullInternal(cdEducationStatus, CD_EDUCATION_STATUS),
        getIntOrNull(cdPurchaseEstimate, CD_PURCHASE_ESTIMATE),
        getStringOrNullInternal(cdCreditRating, CD_CREDIT_RATING),
        getIntOrNull(cdDepCount, CD_DEP_COUNT),
        getIntOrNull(cdDepEmployedCount, CD_DEP_EMPLOYED_COUNT),
        getIntOrNull(cdDepCollegeCount, CD_DEP_COLLEGE_COUNT)))
}
