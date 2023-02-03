#!/bin/bash
LGRAPH_IMPORT=lgraph_import
LGRAPH_DB_DIR=/data/lgraph_db
LGRAPH_TMP_DIR=/data

cd ./load-scripts/import_data
$LGRAPH_IMPORT -c import.conf --dir ${LGRAPH_DB_DIR} --idir ${LGRAPH_TMP_DIR} --overwrite 1 --online 0
