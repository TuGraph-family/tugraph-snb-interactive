#!/bin/bash
HADOOP_HOME="$PWD/deps/hadoop-2.9.2"

if [ -z ${Scale_Factor} ]; then
  echo "Scale_Factor not set"
  echo "try: export Scale_Factor=sf10"
  exit 1
fi

if [ ! -f params-${Scale_Factor}.ini ]; then
  echo "Parameters file (params-${Scale_Factor}.ini) not found."
  exit 1
fi

echo "generate ${Scale_Factor}"

export HADOOP_HOME
export HADOOP_CLIENT_OPTS="-Xms8g -Xmx8g"

cd deps/ldbc_snb_datagen_hadoop
$HADOOP_HOME/bin/hadoop jar target/ldbc_snb_datagen-0.4.0-SNAPSHOT-jar-with-dependencies.jar ../../params-${Scale_Factor}.ini

rm -f m*personFactors*
rm -f .m*personFactors*
rm -f m*activityFactors*
rm -f .m*activityFactors*
rm -f m0friendList*
rm -f .m0friendList*
