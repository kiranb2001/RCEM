#!/bin/bash
# ================================
# Spark Submit Script
# ================================

# Spark memory & core settings
DRIVER_MEMORY=4g
EXECUTOR_MEMORY=8g
EXECUTOR_CORES=4
NUM_EXECUTORS=5

# Path to your PySpark script
PYSPARK_SCRIPT="/path/to/your_script.py"

# Spark submit command
spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-memory ${DRIVER_MEMORY_1} \
  --executor-memory ${EXECUTOR_MEMORY} \
  --executor-cores ${EXECUTOR_CORES} \
  --num-executors ${NUM_EXECUTORS} \
  ${PYSPARK_SCRIPT}
