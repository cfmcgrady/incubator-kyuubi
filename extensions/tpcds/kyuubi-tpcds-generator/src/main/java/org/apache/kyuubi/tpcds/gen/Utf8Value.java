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

import java.nio.charset.StandardCharsets;

/** Cursor-owned UTF-8 storage; dictionary arrays are borrowed without mutation. */
final class Utf8Value {
  byte[] bytes;
  int length;
  private byte[] buffer;
  private String previous;

  private void writable(int capacity) {
    if (buffer == null || buffer.length < capacity) buffer = new byte[Math.max(32, capacity)];
    bytes = buffer;
    previous = null;
  }

  void encode(String value) {
    if (value == previous) return;
    byte[] shared = Distribution.encoded(value);
    if (shared != null) {
      bytes = shared;
      length = shared.length;
    } else {
      // The JDK's bulk encoding is faster than a scalar character loop for generated prose.
      bytes = value.getBytes(StandardCharsets.UTF_8);
      length = bytes.length;
    }
    previous = value;
  }

  void businessKey(long key) {
    writable(16);
    length = 16;
    for (int i = 0; i < 8; i++) {
      bytes[i] = (byte) ('A' + ((key >>> (32 + i * 4)) & 15));
      bytes[i + 8] = (byte) ('A' + ((key >>> (i * 4)) & 15));
    }
  }

  void number(long value, int minimumWidth) {
    // Accumulate in the negative domain so Long.MIN_VALUE is representable.
    long negative = value > 0 ? -value : value;
    int digits = 1;
    for (long rest = negative; rest <= -10; rest /= 10) digits++;
    length = Math.max(minimumWidth, digits + (value < 0 ? 1 : 0));
    writable(length);
    int first = value < 0 ? 1 : 0;
    for (int i = length - 1; i >= first; i--) {
      bytes[i] = (byte) ('0' - negative % 10);
      negative /= 10;
    }
    if (value < 0) bytes[0] = '-';
  }

  @Override
  public String toString() {
    return new String(bytes, 0, length, StandardCharsets.UTF_8);
  }
}
