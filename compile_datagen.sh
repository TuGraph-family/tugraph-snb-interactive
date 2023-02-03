#!/bin/bash
cd deps/ldbc_snb_datagen_hadoop
mvn clean
mvn -DskipTests assembly:assembly
