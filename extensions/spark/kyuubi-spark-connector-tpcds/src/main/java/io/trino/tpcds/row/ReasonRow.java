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
import static io.trino.tpcds.generator.ReasonGeneratorColumn.R_REASON_DESCRIPTION;
import static io.trino.tpcds.generator.ReasonGeneratorColumn.R_REASON_ID;
import static io.trino.tpcds.generator.ReasonGeneratorColumn.R_REASON_SK;

public class ReasonRow
    extends KyuubiTPCDSTableRowWithNulls
{
  private final long rReasonSk;
  private final String rReasonId;
  private final String rReasonDescription;

  public ReasonRow(long nullBitMap, long rReasonSk, String rReasonId, String rReasonDescription)
  {
    super(nullBitMap, R_REASON_SK);
    this.rReasonSk = rReasonSk;
    this.rReasonId = rReasonId;
    this.rReasonDescription = rReasonDescription;
  }

  public long getrReasonSk() {
    return rReasonSk;
  }

  public String getrReasonId() {
    return rReasonId;
  }

  public String getrReasonDescription() {
    return rReasonDescription;
  }

  @Override public Object[] values() {
    return new Object[] {
        getOrNullForKey(rReasonSk, R_REASON_SK),
        getOrNull(rReasonId, R_REASON_ID),
        getOrNull(rReasonDescription, R_REASON_DESCRIPTION)
    };
  }
}
