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

# Kyuubi #7877 与 Spark #60845 的 TPC-DS 大规模数据生成审查

2026-11-25 审查。Kyuubi 和 Spark 对齐了 127 个 TPC-DS 标识列的类型。定向验证确认，Spark 能保留超出 32 位有符号整数上限的票号，Kyuubi 修正后的随机流定位也能在高位分区得到与原生生成器一致的样本。最值得跟进的问题是 **分区切在特定日期边界时，同一笔目录销售订单会偏移一天，即使销售和退货总行数完全相同**。

| 审查对象 | 提交 | 审查时状态 |
| --- | --- | --- |
| [Apache Kyuubi #7877](https://github.com/apache/kyuubi/pull/7877) | [`a33edc7`](https://github.com/apache/kyuubi/commit/a33edc73d2734b33f8e4603dc3b6e596c9ef5e41) | 开放 |
| [Apache Spark #60845](https://github.com/apache/spark/pull/60845) | [`14505c3`](https://github.com/apache/spark/commit/14505c317e869488a00ea413fa3824dd48120d60) | 已纳入 `master` 和 `branch-4.x`。[合并记录](https://github.com/apache/spark/pull/60845#issuecomment-7150544854) |

## 实现差异

| 环节 | Kyuubi | Spark |
| --- | --- | --- |
| 数据来源 | 连接器和 `dev/kyuubi-tpcds` 均使用 `io.trino.tpcds` 1.4。两处现在都按 `Long` 偏移定位当前表及其父表、子表的随机流，并用循环跳过空结果。 | `GenTPCDSData` 调用原生 `dsdgen`，将分隔文本按 `TPCDSSchema` 转型后写出。此 PR 改动的是类型映射和测试数据。 |
| 标识列 | 开发工具将标识列统一为 `LongType`。连接器原已将 `IDENTIFIER` 映射为 `LongType`，这次另行纠正 `cc_closed_date_sk`、`cc_open_date_sk`、`cp_start_date_sk`、`cp_end_date_sk` 四个上游声明为 `INTEGER` 的列。 | DDL 中 127 个标识列改为 `BIGINT`，对应 SF100 列统计的平均长度、最大长度均改为 8 字节。客户最近评审日期键在 Spark 中沿用列名 `c_last_review_date`。 |
| 分区与计划 | 按表的估算字节数和 `maxPartitionBytes` 做精确向上取整，再用 `Math.toIntExact` 检查分区数能否表示为 `Int`。 | 349 个计划预期文件发生变化。SF100 有 6 个计划从广播哈希连接切换为排序合并连接，2 个计划的连接顺序改变。18 个查询结果预期文件中，每种结果行的出现次数均保持一致。 |

实现依据见 [Kyuubi 连接器迭代器](https://github.com/apache/kyuubi/blob/a33edc73d2734b33f8e4603dc3b6e596c9ef5e41/extensions/spark/kyuubi-spark-connector-tpcds/src/main/scala/org/apache/kyuubi/spark/connector/tpcds/KyuubiTPCDSResults.scala#L95)、[Kyuubi 类型修正](https://github.com/apache/kyuubi/blob/a33edc73d2734b33f8e4603dc3b6e596c9ef5e41/extensions/spark/kyuubi-spark-connector-tpcds/src/main/scala/org/apache/kyuubi/spark/connector/tpcds/TPCDSSchemaUtils.scala#L60)和 [Spark 标识列 DDL](https://github.com/apache/spark/blob/14505c317e869488a00ea413fa3824dd48120d60/sql/core/src/test/scala/org/apache/spark/sql/TPCDSSchema.scala#L48)。

## 审查发现与复现方法

### 1. Kyuubi 在目录销售日期边界上依赖分区方式

在 SF100000 读取同一段 200 个订单。整段读取使用 `parallelism=80000000, chunk=15213974`。拆分读取使用 `parallelism=160000000`，依次拼接 `chunk=30427947` 和 `30427948`。只需读取这三个分区。对订单 `3042794701` 得到以下结果。

| 表和日期列 | 整段读取 | 拆分读取 | 两种读取的行数 |
| --- | ---: | ---: | ---: |
| `catalog_sales.cs_sold_date_sk` | 2451171 | 2451172 | 各 1,856 行 |
| `catalog_returns.cr_returned_date_sk` | 2451350 | 2451351 | 各 204 行 |

连接器的 `TPCDSPartitionReader`、开发工具的 `TPCDSRowIterator` 和原生 `dsdgen` 均复现此现象。上游为目录销售单独计算分区首行的日期。在日界线处初始化分区和从前一单顺序生成，可能得到相差一天的日期。按日期分组、过滤或写入日期分区时，这会改变记录归属。该行为来自上游，在评估本次随机流修复时应单独跟踪。[分区日期定位](https://github.com/trinodb/tpcds/blob/1.4/src/main/java/io/trino/tpcds/Parallel.java#L56) · [目录销售生成器](https://github.com/trinodb/tpcds/blob/1.4/src/main/java/io/trino/tpcds/row/generator/CatalogSalesRowGenerator.java#L103)

**建议补测。** 将上述三个分区用作两个 Kyuubi 入口的固定样本，逐值比较订单号、商品键及销售或退货日期。现有跨 32 位边界的逐行比较覆盖 `store_sales` 和 `store_returns`，而 `catalog_*` 的大分区样本只断言行数。上述样本还可用于决定项目是否要求日期在重新分区后保持一致。[现有分区测试](https://github.com/apache/kyuubi/blob/a33edc73d2734b33f8e4603dc3b6e596c9ef5e41/extensions/spark/kyuubi-spark-connector-tpcds/src/test/scala/org/apache/kyuubi/spark/connector/tpcds/TPCDSTableSuite.scala#L72)

### 2. Kyuubi 的计划分区数可包含大量空任务

新增用例将 `sf1.store_sales` 规划为 5 个分区。上游按订单数切分，该表在 SF1 有 240,000 单，低于 100 万的并行阈值。实测第一个分区持有订单 1 至 240,000，后四个分区全空。因此，减小 `maxPartitionBytes` 可以增加调度开销，同时全部生成工作仍落在一个任务上。连接器使用的 `table.isSmall` 与上游按规模判断的阈值不同。[连接器分区规划](https://github.com/apache/kyuubi/blob/a33edc73d2734b33f8e4603dc3b6e596c9ef5e41/extensions/spark/kyuubi-spark-connector-tpcds/src/main/scala/org/apache/kyuubi/spark/connector/tpcds/TPCDSBatchScan.scala#L43) · [上游切分规则](https://github.com/trinodb/tpcds/blob/1.4/src/main/java/io/trino/tpcds/Parallel.java#L24)

**建议补测。** 用超过并行阈值的表验证向上取整和实际分区范围，另用小表验证非空分区数。配置校验也受表类型影响。实测 `maxPartitionBytes=0` 时，`sf1.store_sales` 抛出 `IllegalArgumentException`，`sf1.call_center` 则绕过惰性校验并完成规划。对 `sf10000.store_sales` 设置 1 字节可直接验证分区数超出 `Int` 后的 `ArithmeticException`，无需分配分区数组。[配置校验](https://github.com/apache/kyuubi/blob/a33edc73d2734b33f8e4603dc3b6e596c9ef5e41/extensions/spark/kyuubi-spark-connector-tpcds/src/main/scala/org/apache/kyuubi/spark/connector/tpcds/TPCDSConf.scala#L65) · [现有分区数断言](https://github.com/apache/kyuubi/blob/a33edc73d2734b33f8e4603dc3b6e596c9ef5e41/extensions/spark/kyuubi-spark-connector-tpcds/src/test/scala/org/apache/kyuubi/spark/connector/tpcds/TPCDSTableSuite.scala#L223)

### 3. Spark 的 SF100 列宽与表级大小采用了不同估算

`inventory` 有 399,330,000 行。三个标识列改为每列 8 字节，数量列为 4 字节，加上估算器每行 8 字节开销，得到每行 36 字节。用 PR 的同一份统计调用 `toPlanStats`，启用计划统计得到 **14,375,880,000 字节**，关闭后直接取表级 `sizeInBytes`，得到 **9,583,920,000 字节**，等于每行 24 字节的旧估算。使用表级估算的计划可能因此得到不同的连接代价。[SF100 统计](https://github.com/apache/spark/blob/14505c317e869488a00ea413fa3824dd48120d60/sql/core/src/test/scala/org/apache/spark/sql/TPCDSTableStats.scala#L286) · [统计选择逻辑](https://github.com/apache/spark/blob/14505c317e869488a00ea413fa3824dd48120d60/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/catalog/interface.scala#L921)

**建议补测。** 用 `inventory` 的四个输出列分别调用 `toPlanStats` 的两种模式，并明确表级常量采用的口径。如果目的是表达逻辑行宽，应同步核对其他表的常量。如果目的是保留实测存储大小，应记录其数据来源，便于解释与列宽估算的差异。

### 4. Spark 按日期分区写 Parquet 后，按路径读回的类型会变化

通过 `GenTPCDSData` 的实际转换和写入代码输入两条文本记录，票号分别为 `2147483647` 和 `2147483648`，销售日期键均为 `2451171`。在 Spark 4.2.1 上，ANSI 模式开启和关闭时都能准确读回两个票号。使用 `spark.read.parquet(path)` 读取时，日期键的类型取决于写入方式。

| 写入设置 | 票号读回类型 | 销售日期键读回类型 |
| --- | --- | --- |
| 普通 Parquet | `BIGINT` | `BIGINT` |
| `--partitionTables` | `BIGINT` | `INT` |

分区列由目录名推断，`2451171` 落在 `INT` 范围内。依赖自动推断的消费者因此看到与 TPC-DS DDL 不同的 schema。现有查询测试在建表时直接指定 DDL，不会触发这条读回路径。[生成器写入逻辑](https://github.com/apache/spark/blob/14505c317e869488a00ea413fa3824dd48120d60/sql/core/src/test/scala/org/apache/spark/sql/GenTPCDSData.scala#L241) · [查询测试建表逻辑](https://github.com/apache/spark/blob/14505c317e869488a00ea413fa3824dd48120d60/sql/core/src/test/scala/org/apache/spark/sql/TPCDSQueryTestSuite.scala#L92)

**建议补测。** 将两条边界输入固化为轻量回归用例，覆盖 ANSI 开关、分区与普通 Parquet 写入，同时断言票号的值和读回类型。需要按 DDL 保持分区键类型的消费者可在读取时显式提供 schema。

## 验证范围

审查对照了上述两个 PR 的提交及其差异，使用 Spark 4.2.1 运行定向验证。下表记录了 Kyuubi 开发工具与原生 `dsdgen` 在相同分区和默认种子下的比较结果。统一空值表示后，六组样本逐字段一致。原生工具使用 Spark CI 固定的 [databricks/tpcds-kit 提交](https://github.com/databricks/tpcds-kit/commit/1b7fb7529edae091684201fab142d956d6afd881)。

| 表族 | 规模因子 | 分区总数 | 分区编号 | 销售行数 | 退货行数 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `store_*` | 10,000 | 24,000,000 | 23,000,001 | 1,176 | 121 |
| `catalog_*` | 30,000 | 48,000,000 | 47,999,999 | 863 | 80 |
| `web_*` | 100,000 | 60,000,000 | 59,999,999 | 1,160 | 121 |

本次验证限于选定分区、两条边界文本和统计估算路径，未生成完整的大规模数据集，也未运行两个仓库的完整测试套件。
