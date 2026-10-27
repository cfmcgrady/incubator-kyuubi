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

import java.math.BigInteger;
import org.junit.jupiter.api.Test;

class RandomStreamsTest {
  @Test
  void seekingMatchesIndependentBigIntegerOracle() {
    BigInteger modulus = BigInteger.valueOf(RandomStreams.MODULUS);
    for (long draws : new long[] {0, 1, 91, 2147483647L, 150000000000L, Long.MAX_VALUE}) {
      for (long seed : new long[] {1, 1434868289L, 2147483646L}) {
        long expected =
            BigInteger.valueOf(16807)
                .modPow(BigInteger.valueOf(draws), modulus)
                .multiply(BigInteger.valueOf(seed))
                .mod(modulus)
                .longValueExact();
        assertEquals(expected, RandomStreams.advance(seed, draws));
      }
    }
    assertThrows(IllegalArgumentException.class, () -> RandomStreams.advance(1, -1));
  }

  @Test
  void parentSeekingUsesLongOffsetsAndPerStreamBudgets() {
    long start = 3000000000L;
    RandomStreams expected = new RandomStreams(TpchTable.LINE_ITEM, start);
    RandomStreams resumed = new RandomStreams(TpchTable.LINE_ITEM, start + 1);
    for (int i = 0; i < 7; i++) expected.uniform(14, 1, 50);
    expected.finish();
    for (int i = 0; i < 7; i++)
      assertEquals(expected.uniform(14, 1, 50), resumed.uniform(14, 1, 50));
  }
}
