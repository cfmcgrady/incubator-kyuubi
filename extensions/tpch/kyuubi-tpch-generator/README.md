<!--
- Licensed to the Apache Software Foundation (ASF) under one or more
- contributor license agreements.  See the NOTICE file distributed with
- this work for additional information regarding copyright ownership.
- The ASF licenses this file to You under the Apache License, Version 2.0
- (the "License"); you may not use this file except in compliance with
- the License.  You may obtain a copy of the License at
-
-   http://www.apache.org/licenses/LICENSE-2.0
-
- Unless required by applicable law or agreed to in writing, software
- distributed under the License is distributed on an "AS IS" BASIS,
- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
- See the License for the specific language governing permissions and
- limitations under the License.
-->

# Independent TPC-H generation core

All eight base tables use this JDK-only implementation. The Spark connector
bundles the core and no longer depends on Trino's TPC-H generator. Existing
column names, SQL types (including DOUBLE amounts), ANSI string options, and
zero/tiny catalog behavior are preserved. No native executable or JNI is used
at runtime.

The compatibility reference is dbgen **2.17.3** at
[`gregrahn/tpch-kit@852ad0a5ee31ebefeed884cea4188781dd9613a3`](https://github.com/gregrahn/tpch-kit/tree/852ad0a5ee31ebefeed884cea4188781dd9613a3).
The commit, compiler flags and partition arguments are part of this contract.
This module generates base data; refresh streams and query parameter generation
are outside the connector's existing scope.

## API and implementation

`TpchGenerator.partition(table, scale, partitions, partition)` returns a
`RowCursor`. Partition indices are one-based. `TpchGenerator.range` accepts a
zero-based, half-open range of parent rows: orders for lineitem, parts for
partsupp, and output rows for the other tables. Native dbgen assigns remainder
parents to the last partition. Nation and region are emitted only by the first
connector partition to avoid duplicate dimension rows.

Each cursor owns its random state and reusable primitive buffers. Keys and
random offsets remain 64-bit. Money calculations use cents; the Spark adapter
converts directly to DOUBLE and writes `UnsafeRow` without intermediate strings.
Dates are epoch days. Current values are valid after `next()` returns true and
until the following `next()` call; callers retaining a Spark row must copy it.

VARCHAR values are generated directly as ASCII (valid UTF-8). `getBytes`,
`getByteOffset` and `getByteLength` expose a read-only slice valid for the current
row. Dictionaries are encoded once, comments refer to the shared text pool,
and names, addresses and phone numbers use cursor-owned reusable buffers.
Supplier comment markers are applied to a private copy. Consumers must not
modify backing arrays or retain slices across `next()`. `getString` remains
available for native text output and decodes VARCHAR values on demand.
The Spark reader copies these slices directly into its reusable `UnsafeRow`,
avoiding per-value String and UTF8String allocation and encoding.

The reference's immutable **300 MiB text pool** is initialized lazily once per
JVM and shared by all cursors. Its startup time and heap footprint still apply.
Empty scans do not initialize it. Part names calculate the five observed
permutation positions and skip the remaining random draws. Row budgets detect
accidental extra random consumption.

The core accepts integer SF1 through SF100000, zero as an empty-data extension,
and fractional scales from 0.001 to below 1 using native thousandth truncation.
The connector continues to expose its existing scale catalog.

## Compatibility details

- At SF30000 and above, native dbgen draws customer/part foreign keys with a
  64-bit generator, while partition seeking and unused line-key draws retain
  the older random algorithm. **These key values can depend on partition
  boundaries.** This implementation preserves that behavior for the same
  partition arguments. Splitting a range differently at these scales is not
  promised to reproduce the same keys. Counts remain invariant.
- The reference is compiled with `-fwrapv` to define the signed arithmetic used
  by its address-bit range and mixed random streams. The Java implementation
  preserves those two's-complement operations.
- Line quantity is a DOUBLE in Spark but an integer in native text output.
  Blank text is preserved. No base-table column contains nulls.
- The existing connector's partition-size calculation used row counts where
  bytes were required. It now divides estimated bytes by `maxPartitionBytes`,
  rounds up and rejects nonpositive sizes.

## Validation evidence

`src/test/resources/native/reference-matrix.tsv` records **136 comparisons and
8,769,453 rows**, including all eight full tables at SF0.01 and SF1, and 120
bounded partition samples at SF0.01, SF1, SF10, SF10000, SF30000 and SF100000.
Every output field and row boundary was compared. Large samples include offsets
beyond 32-bit keys and random periods. The manifest and 128 compressed golden
files make the bounded comparisons and full tiny comparisons repeatable offline.

Tests also compare concatenated partitions with full native tiny data, check
range exhaustion and invalid inputs, compare UTF-8 slices with native fields,
verify random seeking against a BigInteger
oracle, and check interleaved cursors and Spark typed conversion. All 22 existing
TPC-H query results on tiny are checked against their existing golden results.

`lineitem-counts.tsv` contains native counts for every connector scale. A separate
native harness enumerates the complete 2,147,483,646-state order-line-count
period and the necessary prefixes. An opt-in Java test independently enumerates
the same period through the new random stream. Each complete period contributes
8,589,934,584 lines. The verified SF10000 count is **59,999,994,267**.
This validates the full line-count sequence, not all field values at 10TB.
No complete 10TB dataset was materialized.

```bash
build/mvn -pl extensions/tpch/kyuubi-tpch-generator -am test
# Include the expensive complete-period count check:
build/mvn -pl extensions/tpch/kyuubi-tpch-generator -am \
  -Dtpch.verifyFullPeriod=true test
```

To reproduce the native comparisons on Linux, clone the reference outside this
repository, check out the pinned commit, and build in its `dbgen` directory:

```bash
make -B -j4 MACHINE=LINUX DATABASE=ORACLE \
  CFLAGS='-O3 -DLINUX -DORACLE -DTPCH -D_FILE_OFFSET_BITS=64 -fcommon -fwrapv -std=gnu89' dbgen
```

Build this core JAR, then run with an output directory outside the checkout:

```bash
python3 extensions/tpch/kyuubi-tpch-generator/tools/verify-native.py \
  --dbgen-dir /path/to/tpch-kit/dbgen \
  --core-jar extensions/tpch/kyuubi-tpch-generator/target/kyuubi-tpch-generator-1.13.0-SNAPSHOT.jar \
  --output-dir /tmp/tpch-reference-validation
```

The script runs the unmodified native binary. Large-table samples use FIFOs and
stop after 100 complete parents. Native dbgen seeks backwards to replace its
last delimiter on regular files; the script removes that delimiter explicitly
for FIFO samples, retaining every field byte. Full tiny and SF1 comparisons use
regular files. The report records the commands, ranges, row counts and SHA-256.
`--skip-full --scales 0.01` provides a smaller reproduction run.

`tools/NativeCounts.c` links against the native object files and uses the native
`dss_random` function. From the native `dbgen` directory, compile it with the
same flags, `-I.`, and the objects `build.o bm_utils.o rnd.o print.o load_stub.o
bcd2.o speed_seed.o text.o permute.o rng64.o`, plus `-lm`. Its two output columns
are parent orders and lineitem count.

## Performance measurement

The opt-in Spark benchmark consumes generated `UnsafeRow` contents for all eight
tables. It performs a warmup before timing; text-pool initialization is excluded.
These are local generation measurements, not end-to-end writes or TPC Benchmark
Results, and are not comparable to TPC Benchmark Results.

```bash
RUN_BENCHMARK=1 build/mvn -pl extensions/spark/kyuubi-spark-connector-tpch -am \
  -Dtest=none -DwildcardSuites=org.apache.spark.kyuubi.TPCHCoreGenerateBenchmark test
```

## Dictionary provenance

The algorithms are independent Java implementations. The standard `dists.dss`
dictionary is retained byte for byte from `io.trino.tpch:tpch:1.1`, which declares
Apache License 2.0. This resource is benchmark data, not executable Trino code.
Its original notices and SHA-256 are retained in
`src/main/resources/META-INF/NOTICE-tpch-dictionary`. Native source and binaries
are used only for external reference validation and are not bundled.
