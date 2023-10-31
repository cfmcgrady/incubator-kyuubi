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
package io.trino.tpcds.row;

import java.util.List;

import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

import static com.google.common.collect.Lists.newArrayList;
import static io.trino.tpcds.generator.IncomeBandGeneratorColumn.IB_INCOME_BAND_ID;
import static io.trino.tpcds.generator.IncomeBandGeneratorColumn.IB_LOWER_BOUND;
import static io.trino.tpcds.generator.IncomeBandGeneratorColumn.IB_UPPER_BOUND;

public class IncomeBandRow
    extends KyuubiTPCDSTableRowWithNulls {
  private final int ibIncomeBandId;
  private final int ibLowerBound;
  private final int ibUpperBound;

  public IncomeBandRow(long nullBitMap, int ibIncomeBandId, int ibLowerBound, int ibUpperBound) {
    super(nullBitMap, IB_INCOME_BAND_ID);
    this.ibIncomeBandId = ibIncomeBandId;
    this.ibLowerBound = ibLowerBound;
    this.ibUpperBound = ibUpperBound;
  }

  public int getIbIncomeBandId() {
    return ibIncomeBandId;
  }

  public int getIbLowerBound() {
    return ibLowerBound;
  }

  public int getIbUpperBound() {
    return ibUpperBound;
  }

  @Override public Object[] values() {
    return new Object[] {
        getOrNull(ibIncomeBandId, IB_INCOME_BAND_ID),
        getOrNull(ibLowerBound, IB_LOWER_BOUND),
        getOrNull(ibUpperBound, IB_UPPER_BOUND)
    };
  }
}
