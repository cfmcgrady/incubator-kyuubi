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

/**
 * A reusable ASCII/UTF-8 slice, with private storage allocated only for generated or edited text.
 */
final class TextValue {
  byte[] bytes;
  int offset;
  int length;
  private final int capacity;
  private byte[] buffer;

  TextValue(int capacity) {
    this.capacity = capacity;
  }

  void set(byte[] value) {
    set(value, 0, value.length);
  }

  void set(byte[] value, int start, int size) {
    bytes = value;
    offset = start;
    length = size;
  }

  void reset() {
    if (buffer == null) buffer = new byte[capacity];
    set(buffer, 0, 0);
  }

  void append(byte value) {
    bytes[length++] = value;
  }

  void append(byte[] value) {
    System.arraycopy(value, 0, bytes, length, value.length);
    length += value.length;
  }

  /** All generated identifiers and phone components are nonnegative. */
  void appendNumber(long value, int minDigits) {
    int digits = 1;
    for (long remaining = value; remaining >= 10; remaining /= 10) digits++;
    int end = length + Math.max(digits, minDigits);
    for (int i = end - 1; i >= length; i--) {
      bytes[i] = (byte) ('0' + value % 10);
      value /= 10;
    }
    length = end;
  }

  void replace(int start, byte[] value) {
    // Supplier markers must never modify the shared reference text pool.
    if (buffer == null) buffer = new byte[capacity];
    if (bytes != buffer) {
      System.arraycopy(bytes, offset, buffer, 0, length);
      bytes = buffer;
      offset = 0;
    }
    System.arraycopy(value, 0, bytes, start, value.length);
  }
}
