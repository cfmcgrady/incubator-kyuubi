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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class RowRangeTest {
  @Test
  void partitionsCoverTheWholeInputWithoutGapsOrOverlap() {
    for (long rows : new long[] {0, 999999, 1000000, 2400000000L, Long.MAX_VALUE}) {
      for (int partitions : new int[] {1, 2, 7, 113}) {
        long end = 0;
        long total = 0;
        for (int partition = 1; partition <= partitions; partition++) {
          RowRange range = RowRange.split(rows, partitions, partition);
          if (range.end() != 0) {
            assertEquals(end, range.start());
            end = range.end();
          }
          total = Math.addExact(total, range.end() - range.start());
        }
        assertEquals(rows, end);
        assertEquals(rows, total);
      }
    }
  }

  @Test
  void invalidPartitionsFailAtTheBoundary() {
    assertThrows(IllegalArgumentException.class, () -> RowRange.split(1000000, 0, 1));
    assertThrows(IllegalArgumentException.class, () -> RowRange.split(1000000, 2, 0));
    assertThrows(IllegalArgumentException.class, () -> RowRange.split(1000000, 2, 3));
    assertThrows(IllegalArgumentException.class, () -> new RowRange(2, 1));
  }
}
