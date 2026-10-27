/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.tpcds.gen;

/** A zero-based, half-open range of parent rows, independent of output line-item counts. */
public final class RowRange {
  private static final long MINIMUM_PARALLEL_ROWS = 1000000L;
  private final long start;
  private final long end;

  public RowRange(long start, long end) {
    if (start < 0 || end < start) {
      throw new IllegalArgumentException("Invalid row range: [" + start + ", " + end + ")");
    }
    this.start = start;
    this.end = end;
  }

  public long start() {
    return start;
  }

  public long end() {
    return end;
  }

  public static RowRange split(long rows, int partitions, int partition) {
    if (rows < 0 || partitions <= 0 || partition < 1 || partition > partitions) {
      throw new IllegalArgumentException("Invalid partition or row count");
    }
    if (rows < MINIMUM_PARALLEL_ROWS) {
      return new RowRange(0, partition == 1 ? rows : 0);
    }
    long quotient = rows / partitions;
    long remainder = rows % partitions;
    long preceding = partition - 1L;
    long start = preceding * quotient + Math.min(preceding, remainder);
    return new RowRange(start, start + quotient + (partition <= remainder ? 1 : 0));
  }
}
