# TuGraph LDBC SNB Manual
SNB is one of the graph database-oriented benchmarks developed by the Linked Data Benchmark Council (LDBC). The SNB not only includes the performance test of read and write queries, but also includes related verification of system transactionality, recoverability, correctness, and stability. It is currently the most mature and general-purpose benchmark in the graph data industry. This document introduces the process of building and running SNB by TuGraph on a centos-like system of ARM architecture. It mainly includes five parts: preparation, loading & preprocessing, benchmark, backup & recovery, and ACID tests. When performing the test, the Driver officially provided by LDBC and TuGraph Server (SUT) is placed on different server instances, and users can test themselves in the same environment.
# 1. Preparation
## 1.1 Download & install dependencies
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
## 1.2 LDBC SNB project compilation
Compile ldbc_snb_datagen, ldbc_snb_driver, and TuGraph SNB connector (ldbc_snb_interactive_impls)
```shell
cd /data/tugraph_ldbc_snb && ./compile_datagen.sh
cd /data/tugraph_ldbc_snb/deps/ldbc_snb_interactive_driver && mvn install -DskipTests
cd /data/tugraph_ldbc_snb/deps/ldbc_snb_interactive_impls && bash build.sh
```
## 1.3 Install TuGraph
```shell
cd /data
rpm -i TuGraph-3.3.4-1.el7.aarch64.rpm
```
# 2. Loading and preprocessing
## 2.1 Data generation
### 2.1.1 Set environment variables
```shell
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.352.b08-2.al8.aarch64
export LD_LIBRARY_PATH=/usr/local/lib64
export DB_ROOT_DIR=/data
export Scale_Factor=sf10
export LC_CTYPE=en_US.UTF-8
export LC_ALL=en_US.UTF-8
```
### 2.1.2 Run
```shell
cd /data/tugraph_ldbc_snb
./run_datagen.sh
```
## 2.2 Load data
```shell
cd /data/tugraph_ldbc_snb
./convert_csvs.sh
./import_data.sh
```
## 2.3 Preprocess db
Perform preprocessing operations on the imported data database, including three steps of expanding foreign keys to vertices, building indexes, and building materialized views.
```shell
cd /data/tugraph_ldbc_snb/plugins
./compile_embedded.sh generate_snb_constants
./generate_snb_constants ${DB_ROOT_DIR}/lgraph_db snb_constants.h
./compile_embedded.sh preprocess
time ./preprocess ${DB_ROOT_DIR}/lgraph_db
```
## 2.4 Install stored procedure
Load 29 stored procedures for interactive workloads into the database
```shell
cd /data/tugraph_ldbc_snb
lgraph_server -c lgraph_standalone.json -d stop --directory ${DB_ROOT_DIR}/lgraph_db
lgraph_server -c lgraph_standalone.json -d start --directory ${DB_ROOT_DIR}/lgraph_db
cd /data/tugraph_ldbc_snb/plugins
bash install.sh
```
# 3. Benchmark
## 3.1 Validate
Lay the data of sf10, use the result file obtained by executing the interactive workload on Neo4j as the validation data set, and verify the correctness of the TuGraph execution query.
### 3.1.1 Create validation
```shell
# create validation
cd /data/tugraph_ldbc_snb/deps/ldbc_snb_interactive_impls/tugraph && sync
bash run.sh interactive-create-validation-parameters.properties
# stop server
cd /data/tugraph_ldbc_snb
lgraph_server -c lgraph_standalone.json -d stop --directory ${DB_ROOT_DIR}/lgraph_db
```
### 3.1.2 Validate
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
## 3.2 Run benchmark
For the databases of sf30, sf100 and sf300, first use the lgraph_warmup tool provided by TuGraph to warm up the database into the memory, and then run the benchmark. The results are shown in Section 7.
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
## 3.3 Check consistency
After executing the tests, verify the consistency of the database materialized view results.
```shell
cd /data/tugraph_ldbc_snb/plugins
./compile_embedded.sh check_consistency
./check_consistency ${DB_ROOT_DIR}/lgraph_db
```
# 4. Backup & Restore
You can backup and restore the database before/after some steps to avoid performing all steps repeatedly.
## 4.1 Backup db
```shell
cd /data/tugraph_ldbc_snb
lgraph_server -c lgraph_standalone.json -d stop --directory ${DB_ROOT_DIR}/lgraph_db
rm -r ${DB_ROOT_DIR}/lgraph_db.bak
cp -r ${DB_ROOT_DIR}/lgraph_db ${DB_ROOT_DIR}/lgraph_db.bak
lgraph_server -c lgraph_standalone.json -d start --directory ${DB_ROOT_DIR}/lgraph_db
```
## 4.2 Restore db
```shell
cd /data/tugraph_ldbc_snb/
lgraph_server -c lgraph_standalone.json -d stop --directory ${DB_ROOT_DIR}/lgraph_db
rm -r ${DB_ROOT_DIR}/lgraph_db
cp -r ${DB_ROOT_DIR}/lgraph_db.bak ${DB_ROOT_DIR}/lgraph_db
lgraph_server -c lgraph_standalone.json -d start --directory ${DB_ROOT_DIR}/lgraph_db
```
# 5. ACID Tests
## 5.1 Compile & run ACID tests
ACID test implementations are reviewed for compliance with the ACID test specification and implement all specified test cases. Additionally, tests execute successfully without atomicity and isolation test failures, supporting serializable isolation level transaction settings.
```shell
cd /data/tugraph_ldbc_snb/plugins
./compile_embedded.sh acid
./acid
```
## 5.2 Durability test
The persistence test uses the sf30 routine benchmark workload. Two machines were forced to restart (ungracefully) 2 hours after the benchmark started. After restarting the machines, the server process was successfully restarted within 20ms, and the last 8 update queries before the restart were successfully queried, which proves that all updates have been successfully persisted to the hard disk and are not affected by machine crashes.
```shell
sudo shutdown -rf hh:mm
cd /data/tugraph_ldbc_snb
lgraph_server -c lgraph_standalone.json -d start --directory ${DB_ROOT_DIR}/lgraph_db && sleep 10s
cd /data/tugraph_ldbc_snb/plugins
./compile_embedded.sh recovery_queries
./recovery_queries ${DB_ROOT_DIR}/lgraph_db
```
# 6. Results

Recently, TuGraph passed the Audit of LDBC SNB Interactive on the domestic ARM architecture platform, and its specific machine environment is shown in the table below.

|  Machine type  |                                                                                                                                 Alibaba Cloud ecs.g8y.16xlarge                                                                                                                                 |
|:--------------:|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
|      CPU       |                                                                                                                                ARM-based YiTian 710，64核，2.75GHz                                                                                                                                |
|     Memory     |                                                                                                                                             256GB                                                                                                                                              |
|      Disk      |                                                                                       NVMe SSD(cloud disk)，ext4，The 4KB QD1 write performance measured with the fio command was an average of 3431 IOPS.                                                                                       |
|    Network     | Driver and SUT were assigned to the same VPC with the same subnetwork 172.16.16.0/20. Network throughput between the two instances measured using the iperf tool on port 9091 using 48 threads were an average of 33.4 Gbit/sec from client to server and 31.6 Gbit/sec from server to client. |
|       OS       |                                                                                                                             Alibaba Cloud Linux 3 (Soaring Falcon)                                                                                                                             |

According to the official report released by LDBC(https://ldbcouncil.org/benchmarks/snb/LDBC_SNB_I_20230128_SF30-100-300_tugraph.pdf), the performance of TuGraph on the sf30, sf100 and sf300 data sets has set a new world record, and its throughput has increased by 32%, 31% and 6% respectively compared with the previous audit. The specific results are shown in the following table .
In testing we found:
- Since NVMe SSD is a cloud disk, the performance of SNB is greatly affected by network fluctuations.
- The data scale of sf300 exceeds the memory capacity, which is the key bottleneck restricting its SNB performance. But despite this, the SNB performance of sf300 still achieved 6% better performance than the last audit.

|  Scale factor  |  Benchmark duration  |  Benchmark operations  | Throughput  |  Query on-time compliance  |
|:--------------:|:--------------------:|:----------------------:|:-----------:|:--------------------------:|
|      sf30      |   02h 01m 29.605s    |      117,603,754       |  16,133.08  |           99.98%           |
|     sf100      |   02h 00m 26.624s    |      122,608,813       |  16,966.26  |           95.63%           |
|     sf300      |   02h 02m 51.486s    |       99,755,499       |  13 532.62  |           96.31%           |

# 7. Notes
## 7.1 Data Types

Data types used in SNB are implemented with the following mappings:
- ID: `INT64`
- 32-bit integer: `INT32`
- 64-bit integer: `INT64`
- String, Long String, Text: `STRING`
- Date, DateTime: `INT64`
    - [Date](https://github.com/HowardHinnant/date/) is used for Date/DateTime arithmetics
- List of Strings: `STRING`
    - Items are separated via ';'

## 7.2 Data Schema

The schema can be inferred from `import.conf`.
Some traits:
- 1-to-1 and 1-to-N relationships are inlined as vertex properties (like foreign keys).
- Some relationships are further refined according to the connecting entity types:
    - `hasTag`: `forumHasTag`, `postHasTag`, `commenthasTag`
    - `hasCreator`: `postHasCreator`, `commentHasCreator`
    - `isLocatedIn`: `personIsLocatedIn`, `postIsLocatedIn`, `commentIsLocatedIn`
- There are two precomputed edge properties (similar to materialized views):
    - `hasMember.numPosts` which maintains the number of posts the given person posted in the given forum (used in Complex Read 5)
    - `knows.weight` which maintains the weight between the pair of given persons, calculated using the formula in Complex Read 14

### 7.2.1 Indexes

Unique indexes are defined on:
- `id` fields of all vertices (built automatically during data import)
- `name` fields of `TagClass` and `Tag`

A non-unique index is defined on `Place.name`.

## 7.3 Data Generation

In addition to using the `CsvCompositeMergeForeign` classes for serialization, TuGraph uses `LongDateFormatter` for Date/DateTime, and specifies `numUpdatePartitions` to generate multiple update streams.

## 7.4 Bulk Load

The bulk load phase consists of three steps:
- Data preparation: a text processing script converts the csv files generated by `datagen` to a form that the TuGraph import utility requires.
- Importing: `lgraph_import` is used to import the initial dataset. `id` indexes are built during this period.
- Preprocessing: `preprocess` is executed which performs the following actions:
    - Converting foreign key fields to actual vertex identifiers
    - Building those `name` indexes
    - Materializing `hasMember.numPosts` and `knows.weight`

## 7.5 Stored Procedures

All the operations are implemented with stored procedures using TuGraph Core API.
Read (both complex and short) operations are marked as Read-Only while update operations are marked as Read-Write.

Besides the insertions defined in the specification document, Update {5, 6} and Update {7, 8} contain additional logics for maintenance of the two precomputed edge properties.
`check_consistency` can be used for checking the consistency of materialization.

## 7.6 ACID Tests

`acid` contains test cases detecting different types of anomalies.
Using optimistic mode of read-write transactions is able to pass all but Write Skew tests (the first run).
Using pessimistic mode of read-write transactions is able to pass all tests (the second run).

Write skews can be avoided in optimistic mode by generating write-write conflicts explicitly (i.e. adding the read set for validation as well). Some read-write stored procedures use this technique to ensure consistency.
