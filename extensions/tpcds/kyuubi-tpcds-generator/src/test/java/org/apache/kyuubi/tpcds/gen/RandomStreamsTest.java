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

import java.math.BigInteger;
import org.junit.jupiter.api.Test;

class RandomStreamsTest {
  @Test
  void seekMatchesAnArbitraryPrecisionOracleWithoutMultiplicationOverflow() {
    BigInteger modulus = BigInteger.valueOf(Integer.MAX_VALUE);
    BigInteger multiplier = BigInteger.valueOf(16807);
    int column = 337;
    BigInteger initial = BigInteger.valueOf(19620718L + column * (Integer.MAX_VALUE / 799L));
    for (long rows : new long[] {0, 1, 2147483646L, 2147483648L, 24000000000L, Long.MAX_VALUE}) {
      for (int budget : new int[] {0, 1, 16, 128, 1000}) {
        BigInteger draws =
            BigInteger.valueOf(rows).multiply(BigInteger.valueOf(budget)).add(BigInteger.ONE);
        long expected =
            initial.multiply(multiplier.modPow(draws, modulus)).mod(modulus).longValue();
        RandomStreams streams = new RandomStreams(column, new int[] {budget}, rows);
        assertEquals(
            expected,
            streams.uniform(column, 0, Integer.MAX_VALUE - 1L),
            "rows=" + rows + " budget=" + budget);
      }
    }
  }

  @Test
  void variableDrawCountsDoNotChangeTheNextParentRowsStreams() {
    RandomStreams sequential = new RandomStreams(314, new int[] {2, 32}, 0);
    for (int row = 0; row < 100; row++) {
      RandomStreams sought = new RandomStreams(314, new int[] {2, 32}, row);
      assertEquals(sought.uniform(314, 0, 10000), sequential.uniform(314, 0, 10000));
      assertEquals(sought.uniform(315, 0, 10000), sequential.uniform(315, 0, 10000));
      for (int i = 0; i < row % 31; i++) {
        sequential.uniform(315, 0, 10000);
      }
      sequential.finishRow();
    }
  }

  @Test
  void drawBudgetViolationsFailInsteadOfCorruptingFollowingRows() {
    RandomStreams stream = new RandomStreams(323, new int[] {1}, 0);
    stream.uniform(323, 8, 16);
    stream.uniform(323, 8, 16);
    assertThrows(IllegalStateException.class, stream::finishRow);
    assertThrows(IllegalArgumentException.class, () -> new RandomStreams(323, new int[] {1}, -1));
  }
}
