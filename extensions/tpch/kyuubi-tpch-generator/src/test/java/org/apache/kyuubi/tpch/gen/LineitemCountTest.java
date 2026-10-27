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

package org.apache.kyuubi.tpch.gen;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.Test;

class LineitemCountTest {
  /**
   * Expensive verification of the entire random period; independent native counts are checked in.
   */
  @Test
  void standardScaleCountsMatchNativeFullPeriod() throws Exception {
    assumeTrue(Boolean.getBoolean("tpch.verifyFullPeriod"));
    long period = RandomStreams.PERIOD;
    List<long[]> checkpoints = new ArrayList<>();
    try (BufferedReader input =
        TpchGeneratorTest.reader(TpchGeneratorTest.resource("lineitem-counts.tsv"))) {
      String line;
      while ((line = input.readLine()) != null) {
        if (line.startsWith("#") || line.isBlank()) continue;
        String[] fields = line.split("\t");
        checkpoints.add(new long[] {Long.parseLong(fields[0]), Long.parseLong(fields[1]), 0});
      }
    }
    checkpoints.sort(Comparator.comparingLong(point -> point[0] % period));
    RandomStreams random = new RandomStreams(TpchTable.LINE_ITEM, 0);
    long count = 0;
    int next = 0;
    for (long i = 1; i <= period; i++) {
      count += random.uniform(43, 1, 7);
      if (next < checkpoints.size() && i == checkpoints.get(next)[0] % period)
        checkpoints.get(next++)[2] = count;
    }
    assertEquals(period * 4, count);
    assertEquals(checkpoints.size(), next);
    for (long[] point : checkpoints)
      assertEquals(point[1], point[0] / period * count + point[2], "Parent rows " + point[0]);
  }
}
