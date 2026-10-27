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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import org.apache.kyuubi.tpch.gen.*;

public class NativeComparison {
  public static void main(String[] args) throws Exception {
    for (String line : Files.readAllLines(Path.of(args[0]))) {
      if (line.startsWith("#") || line.isBlank()) continue;
      String[] p = line.split("\t");
      var t = TpchTable.getTable(p[0]);
      double scale = Double.parseDouble(p[1]);
      RowCursor rows =
          TpchGenerator.range(t, scale, new RowRange(Long.parseLong(p[2]), Long.parseLong(p[3])));
      try (var expected =
          new BufferedReader(
              new InputStreamReader(new FileInputStream(p[4]), StandardCharsets.US_ASCII))) {
        long count = 0;
        String want;
        while ((want = expected.readLine()) != null) {
          if (!rows.next()) throw new AssertionError(line + " premature EOF " + count);
          var actual = new StringBuilder();
          for (int i = 0; i < rows.columnCount(); i++) {
            if (i > 0) actual.append('|');
            String value = rows.getString(i);
            if (t.column(i).getKind() == TpchTable.Kind.VARCHAR) {
              String bytes =
                  new String(
                      rows.getBytes(i),
                      rows.getByteOffset(i),
                      rows.getByteLength(i),
                      StandardCharsets.UTF_8);
              if (!value.equals(bytes))
                throw new AssertionError(line + " row=" + count + " UTF-8 column=" + i);
            }
            actual.append(value);
          }
          if (!want.contentEquals(actual))
            throw new AssertionError(
                line + " row=" + count + "\nexpected=" + want + "\nactual  =" + actual);
          count++;
        }
        if (rows.next()) throw new AssertionError(line + " extra row " + count);
        System.out.println(
            t + " sf=" + scale + " start=" + p[2] + " end=" + p[3] + " rows=" + count + " PASS");
      }
    }
  }
}
