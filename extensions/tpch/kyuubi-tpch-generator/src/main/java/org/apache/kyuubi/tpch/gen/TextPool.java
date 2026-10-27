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

import static org.apache.kyuubi.tpch.gen.StreamIds.*;

/** Lazily initialized, immutable 300 MiB reference text pool shared by all cursors in a JVM. */
final class TextPool {
  static final int SIZE = 300 * 1024 * 1024;

  private static final class Holder {
    static final byte[] BYTES = build();
  }

  static void comment(RandomStreams random, int stream, int average, TextValue target) {
    int min = (int) (average * 0.4), max = (int) (average * 1.6);
    int offset = (int) random.uniform(stream, 0, SIZE - max);
    int length = (int) random.uniform(stream, min, max);
    target.set(Holder.BYTES, offset, length);
  }

  private static byte[] build() {
    byte[] pool = new byte[SIZE];
    RandomStreams random = new RandomStreams(TpchTable.REGION, 0);
    StringBuilder sentence = new StringBuilder(256);
    int offset = 0;
    while (offset < SIZE) {
      sentence.setLength(0);
      String grammar = Distributions.get("grammar").pick(random, TEXT);
      for (int i = 0; i < grammar.length(); i++) {
        switch (grammar.charAt(i)) {
          case 'N':
            phrase(sentence, random, false);
            break;
          case 'V':
            phrase(sentence, random, true);
            break;
          case 'P':
            sentence.append(Distributions.get("prepositions").pick(random, TEXT)).append(" the ");
            phrase(sentence, random, false);
            break;
          case 'T':
            sentence.setLength(sentence.length() - 1);
            sentence.append(Distributions.get("terminators").pick(random, TEXT));
            break;
          default:
            break;
        }
      }
      sentence.append(' ');
      int size = Math.min(sentence.length(), SIZE - offset);
      for (int i = 0; i < size; i++) pool[offset + i] = (byte) sentence.charAt(i);
      offset += size;
    }
    return pool;
  }

  private static void phrase(StringBuilder target, RandomStreams random, boolean verb) {
    String syntax = Distributions.get(verb ? "vp" : "np").pick(random, TEXT);
    for (int i = 0; i < syntax.length(); i++) {
      char token = syntax.charAt(i);
      if (token == ' ') continue;
      String name;
      switch (token) {
        case 'N':
          name = "nouns";
          break;
        case 'J':
          name = "adjectives";
          break;
        case 'D':
          name = "adverbs";
          break;
        case 'A':
          name = "articles";
          break;
        case 'V':
          name = "verbs";
          break;
        case 'X':
          name = "auxillaries";
          break;
        default:
          throw new IllegalStateException("Invalid grammar token: " + token);
      }
      target.append(Distributions.get(name).pick(random, TEXT));
      if (i + 1 < syntax.length() && syntax.charAt(i + 1) != ' ') target.append(syntax.charAt(++i));
      target.append(' ');
    }
  }

  private TextPool() {}
}
