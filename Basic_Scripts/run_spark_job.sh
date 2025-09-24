#!/bin/bash
# ================================
# Spark Submit Script
# ================================

# Spark memory & core settings
DRIVER_MEMORY=2g
EXECUTOR_MEMORY=4g
EXECUTOR_CORES=2
NUM_EXECUTORS=4

# Path to your PySpark script
PYSPARK_SCRIPT="/path/to/your_script.py"

# Spark submit command
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory ${DRIVER_MEMORY} \
  --executor-memory ${EXECUTOR_MEMORY} \
  --executor-cores ${EXECUTOR_CORES} \
  --num-executors ${NUM_EXECUTORS} \
  ${PYSPARK_SCRIPT}
