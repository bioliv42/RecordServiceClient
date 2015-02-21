#!/bin/bash -xe
########################################################################
# Copyright (c) 2014 Cloudera, Inc.
# Confidential Cloudera Information: Covered by NDA.

# This script pulls data from the perf database and uses R to generate
# the results plot.

generate_report() {
  local WORKLOAD=$1
  local DAYS=$2
  local OUTPUT=$3
  ./collect_benchmark_results.py "$WORKLOAD" $DAYS > /tmp/data.tsv
  Rscript generate_runtime_plots.R /tmp/data.tsv $OUTPUT
}

generate_report "Query 1 (Text/6gb)" 10 "Query1_Text_6GB"
generate_report "Query 1 (Parquet/6gb)" 10 "Query1_Parquet_6GB"

mkdir -p $RECORD_SERVICE_HOME/benchmark_results
rm -f $RECORD_SERVICE_HOME/benchmark_results/*
mv ./*.png $RECORD_SERVICE_HOME/benchmark_results
