# TuGraph LDBC SNB 手册
SNB是由关联数据基准委员会（LDBC）开发的面向图数据库的基准测试（Benchmark）之一。 SNB测试不仅包含了读写查询的性能测试，还包含了系统事务性、可恢复性、正确性、稳定性的相关验证，是目前图数据行业最成熟和通用的基准测试。本文档介绍了TuGraph在ARM架构的类centos系统上构建和运行SNB的流程，主要包括测试环境准备、数据导入及预处理、性能测试、备份恢复和ACID测试等五个部分，执行测试时将LDBC官方提供的Driver和TuGraph Server（SUT）放在不同的服务器实例上，用户可以在相同环境下自行测试。
# 1. 测试环境准备
## 1.1 依赖安装
```shell
yum update
yum install -y java-1.8.0-openjdk* git g++
ln -sf /usr/bin/python2 /usr/bin/python 
python -m pip install requests

# copy tugraph_ldbc_snb to /data directory
cp -r ${TUGRAPH_LDBC_SNB_PATH}/tugraph_ldbc_snb /data

# download hadoop
mkdir -p /data/tugraph_ldbc_snb/deps && cd /data/tugraph_ldbc_snb/deps
wget http://archive.apache.org/dist/hadoop/core/hadoop-2.9.2/hadoop-2.9.2.tar.gz
tar xf hadoop-2.9.2.tar.gz

# download maven
cd /data/tugraph_ldbc_snb/deps
wget https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.6.0/apache-maven-3.6.0-bin.tar.gz
tar xf apache-maven-3.6.0-bin.tar.gz && export MAVEN_HOME=/data/tugraph_ldbc_snb/deps/apache-maven-3.6.0 && export PATH=${PATH}:${MAVEN_HOME}/bin

# download hopscotch-map & date
cd /data/tugraph_ldbc_snb/deps && git clone https://github.com/Tessil/hopscotch-map
cd /data/tugraph_ldbc_snb/deps && git clone https://github.com/HowardHinnant/date

# downlaod ldbc_snb_datagen_hadoop
cd /data/tugraph_ldbc_snb/deps && git clone https://github.com/ldbc/ldbc_snb_datagen_hadoop.git && cd ldbc_snb_datagen_hadoop && git checkout v0.3.2

# donwload ldbc_snb_interactive_driver
cd /data/tugraph_ldbc_snb/deps && git clone https://github.com/ldbc/ldbc_snb_interactive_driver.git && cd ldbc_snb_interactive_driver && git checkout v1.2.0

# donwload ldbc_snb_interactive_impls
cd /data/tugraph_ldbc_snb/deps && git clone https://github.com/TuGraph-db/ldbc_snb_interactive_impls.git && cd ldbc_snb_interactive_impls && git checkout audit2023
```
## 1.2 测试项目编译
对ldbc_snb_datagen, ldbc_snb_driver, 和TuGraph SNB connector (ldbc_snb_interactive_impls)进行编译
```shell
cd /data/tugraph_ldbc_snb && ./compile_datagen.sh
cd /data/tugraph_ldbc_snb/deps/ldbc_snb_interactive_driver && mvn install -DskipTests
cd /data/tugraph_ldbc_snb/deps/ldbc_snb_interactive_impls && bash build.sh
```
## 1.3 TuGraph安装
```shell
cd /data
rpm -i TuGraph-3.3.4-1.el7.aarch64.rpm
```
# 2. 数据导入及预处理
## 2.1 数据生成
### 2.1.1 设置环境变量
```shell
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.352.b08-2.al8.aarch64
export LD_LIBRARY_PATH=/usr/local/lib64
export DB_ROOT_DIR=/data
export Scale_Factor=sf10
export LC_CTYPE=en_US.UTF-8
export LC_ALL=en_US.UTF-8
```
### 2.1.2 生成数据
```shell
cd /data/tugraph_ldbc_snb
./run_datagen.sh
```
## 2.2 数据导入
```shell
cd /data/tugraph_ldbc_snb
./convert_csvs.sh
./import_data.sh
```
## 2.3 预处理
对导入数据的数据库执行预处理操作，包括将外键扩展为顶点、建立索引和建立物化视图等三个步骤
```shell
cd /data/tugraph_ldbc_snb/plugins
./compile_embedded.sh generate_snb_constants
./generate_snb_constants ${DB_ROOT_DIR}/lgraph_db snb_constants.h
./compile_embedded.sh preprocess
time ./preprocess ${DB_ROOT_DIR}/lgraph_db
```
## 2.4 加载存储过程
将交互式工作负载的29个查询存储过程加载到数据库中
```shell
cd /data/tugraph_ldbc_snb
lgraph_server -c lgraph_standalone.json -d stop --directory ${DB_ROOT_DIR}/lgraph_db
lgraph_server -c lgraph_standalone.json -d start --directory ${DB_ROOT_DIR}/lgraph_db
cd /data/tugraph_ldbc_snb/plugins
bash install.sh
```
# 3. 性能测试
## 3.1 正确性验证
铺底sf10的数据，以在Neo4j上执行交互式工作负载得到的结果文件作为验证数据集，验证TuGraph执行查询的正确性
### 3.1.1 生成结果文件
```shell
# create validation
cd /data/tugraph_ldbc_snb/deps/ldbc_snb_interactive_impls/tugraph && sync
bash run.sh interactive-create-validation-parameters.properties
# stop server
cd /data/tugraph_ldbc_snb
lgraph_server -c lgraph_standalone.json -d stop --directory ${DB_ROOT_DIR}/lgraph_db
```
### 3.1.2 结果验证
```shell
# copy (neo4j) validation_params.csv
cd /data/tugraph_ldbc_snb/deps/ldbc_snb_interactive_impls/tugraph && sync
cp ${VALIDATION_PARAMS_PATH}/validation_params.csv ./
# validate
bash run.sh interactive-validate.properties
# stop server
cd /data/tugraph_ldbc_snb
lgraph_server -c lgraph_standalone.json -d stop --directory ${DB_ROOT_DIR}/lgraph_db
```
## 3.2 执行性能测试
针对sf30，sf100和sf300的数据库，先用TuGraph提供的lgraph_warmup工具将数据库预热到内存中，然后执行性能测试，测试结果如第7节所示。
```shell
# warmup db
lgraph_warmup -d ${DB_ROOT_DIR}/lgraph_db -g default
# run benchmark
cd /data/tugraph_ldbc_snb/deps/ldbc_snb_interactive_impls/tugraph && sync
bash run.sh interactive-benchmark-${Scale_Factor}.properties
# stop server
cd /data/tugraph_ldbc_snb
lgraph_server -c lgraph_standalone.json -d stop --directory ${DB_ROOT_DIR}/lgraph_db
```
## 3.3 一致性检验
执行完测试之后，验证数据库物化视图结果的一致性。
```shell
cd /data/tugraph_ldbc_snb/plugins
./compile_embedded.sh check_consistency
./check_consistency ${DB_ROOT_DIR}/lgraph_db
```
# 4. 备份及恢复
测试过程中可以在某些步骤之前/之后备份和恢复数据库，以避免重复执行所有步骤。
## 4.1 备份数据库
```shell
cd /data/tugraph_ldbc_snb
lgraph_server -c lgraph_standalone.json -d stop --directory ${DB_ROOT_DIR}/lgraph_db
rm -r ${DB_ROOT_DIR}/lgraph_db.bak
cp -r ${DB_ROOT_DIR}/lgraph_db ${DB_ROOT_DIR}/lgraph_db.bak
lgraph_server -c lgraph_standalone.json -d start --directory ${DB_ROOT_DIR}/lgraph_db
```
## 4.2 恢复数据库
```shell
cd /data/tugraph_ldbc_snb/
lgraph_server -c lgraph_standalone.json -d stop --directory ${DB_ROOT_DIR}/lgraph_db
rm -r ${DB_ROOT_DIR}/lgraph_db
cp -r ${DB_ROOT_DIR}/lgraph_db.bak ${DB_ROOT_DIR}/lgraph_db
lgraph_server -c lgraph_standalone.json -d start --directory ${DB_ROOT_DIR}/lgraph_db
```
# 5. ACID测试
## 5.1 交互式事务ACID测试
ACID 测试实现经过审查已符合 ACID 测试规范，并实现了所有指定的测试用例。此外，测试执行成功，没有原子性和隔离测试失败，支持可序列化隔离级别事务设置。
```shell
cd /data/tugraph_ldbc_snb/plugins
./compile_embedded.sh acid
./acid
```
## 5.2 持久性测试
持久性测试使用sf30常规基准测试工作负载，在基准测试开始2小时后强制重启（ungracefully）两台机器，重启机器后在20ms时间内成功重启服务器进程，并成功查询到重启前最后8个更新查询的信息，证明所有更新已成功持久化到硬盘中，不受机器崩溃的影响。
```shell
sudo shutdown -rf hh:mm
cd /data/tugraph_ldbc_snb
lgraph_server -c lgraph_standalone.json -d start --directory ${DB_ROOT_DIR}/lgraph_db && sleep 10s
cd /data/tugraph_ldbc_snb/plugins
./compile_embedded.sh recovery_queries
./recovery_queries ${DB_ROOT_DIR}/lgraph_db
```
# 6. 测试结果

近日，TuGraph在国产ARM架构平台上通过了LDBC SNB Interactive的Audit，其具体机器环境如下表所示。

| 机器型号  |                            Alibaba Cloud ecs.g8y.16xlarge                            |
|:-----:|:------------------------------------------------------------------------------------:|
|  CPU  |                           ARM-based YiTian 710，64核，2.75GHz                           |
|  内存   |                                        256GB                                         |
|  硬盘   |                    NVMe SSD（云盘），ext4，使用fio测得4KB QD1写入性能为3431 IOPS                    |
|  网络   | Driver和SUT部署在172.16.16.0/20子网中，使用48线程iperf测得client和server之间的数据传输速度分别为33.4G/s和31.6G/s |
| 操作系统  |                        Alibaba Cloud Linux 3 (Soaring Falcon)                        |

据LDBC官方发布的报告(https://ldbcouncil.org/benchmarks/snb/LDBC_SNB_I_20230128_SF30-100-300_tugraph.pdf)，TuGraph在sf30，sf100和sf300数据集上的性能均刷新了世界纪录，其吞吐量分别比上次审计提升了32%、31%和6%，具体结果如下表所示。
我们在测试中发现：
- 由于NVMe SSD为云盘，故SNB的性能受网络波动影响较大。
- sf300的数据规模超过内存容量是制约其SNB性能的关键瓶颈。但尽管如此，sf300的SNB性能仍然取得了超出上次审计6%的表现。

|  数据规模  |       测试时长       |     查询数量     |    吞吐量     |  查询准时率  |
|:------:|:----------------:|:------------:|:----------:|:-------:|
|  sf30  | 02h 01m 29.605s  | 117,603,754  | 16,133.08  | 99.98%  |
| sf100  | 02h 00m 26.624s  | 122,608,813  | 16,966.26  | 95.63%  |
| sf300  | 02h 02m 51.486s  |  99,755,499  | 13 532.62  | 96.31%  |

# 7. 注释
## 7.1 数据类型

SNB 中使用的数据类型通过以下映射实现：
- ID: `INT64`
- 32-bit integer: `INT32`
- 64-bit integer: `INT64`
- String, Long String, Text: `STRING`
- Date, DateTime: `INT64`
  - [Date](https://github.com/HowardHinnant/date/) is used for Date/DateTime arithmetics
- List of Strings: `STRING`
  - Items are separated via ';'

## 7.2 数据模式

数据模式可以从`import.conf`中推断出来。
一些traits:
- 1 对 1 和 1 对 N 关系被内联为顶点属性（如外键）。
- 一些关系根据连接的实体类型进一步细化：
  - `hasTag`: `forumHasTag`, `postHasTag`, `commenthasTag`
  - `hasCreator`: `postHasCreator`, `commentHasCreator`
  - `isLocatedIn`: `personIsLocatedIn`, `postIsLocatedIn`, `commentIsLocatedIn`
- 有两个预先计算的边缘属性（类似于物化视图）：
  - `hasMember.numPosts` 维护给定人员在给定论坛中发布的帖子数（在 Complex Read 5 中使用）
  - `knows.weight` 维持给定人对之间的权重，使用 Complex Read 14 中的公式计算

### 7.2.1 索引

唯一索引定义于：
-  所有顶点的`id`字段（在数据导入期间自动构建）
- `TagClass` 和 `Tag` 的 `name` 字段

在`Place.name`上定义了一个非唯一索引。

## 7.3 数据生成

除了使用`CsvCompositeMergeForeign`类进行序列化之外，TuGraph 还使用`LongDateFormatter`处理日期/日期时间，并指定`numUpdatePartitions`来生成多个更新流。

## 7.4 批量加载

批量加载阶段包括三个步骤：
- 数据准备：文本处理脚本将 `datagen` 生成的 csv 文件转换为 TuGraph 导入所需的格式。
- 导入：`lgraph_import` 用于导入初始数据集。 `id` 索引就是在这个时期建立起来的。
- 预处理：`preprocess`执行以下操作：
  - 将外键字段转换为实际的顶点
  - 建立`name`索引
  - 实体化`hasMember.numPosts` 和 `knows.weight`

## 7.5 存储过程

所有操作都是使用 TuGraph Core API 通过存储过程实现的。
读操作（complex和short）被标记为只读存储过程，而更新操作被标记为读写存储过程。

除了规范文档中定义的插入之外，Update {5, 6} 和 Update {7, 8} 还包含用于维护两个预先计算的边缘属性的附加逻辑。
`check_consistency` 可用于检查物化的一致性。

## 7.6 ACID测试

`acid` 包含检测不同类型异常的测试用例。
使用读写事务的乐观模式能够通过除写偏斜测试（第一次运行）之外的所有测试。
使用悲观模式的读写事务能够通过所有测试（第二次运行）。

通过显式生成写-写冲突（例如添加读取集以进行验证），可以在乐观模式下避免写偏斜。一些读写存储过程使用这种技术来确保一致性。
