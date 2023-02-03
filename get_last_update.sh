CurDir=$PWD
for i in `seq 1 8`; do
    cd ${CurDir}/deps/ldbc_snb_interactive_impls/lightgraph/results
    ts=$(grep LdbcUpdate${i} LDBC-SNB-results_log.csv | tail -n 1 | awk -F '|' '{print $6}')
    cd ${CurDir}/deps/ldbc_snb_datagen_hadoop/social_network
    echo "---Update${i}---"
    grep "^${ts}" updateStream_*
done
