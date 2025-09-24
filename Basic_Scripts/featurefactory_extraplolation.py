from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, date_add

# Define the Paths :
input_path =  "/user/rcemojo/output/ssisqi/callsig/combine/rcem_callsig_dly"
output_path = "/user/rcemojo/output/ssisqi_performance/mapsig/combine/rcem_map_fat_dly"

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("ORC Processing with Partition") \
    .getOrCreate()

# 2. Read ORC file
df = spark.read.orc(input_path)

# 3. Define start and end (number of days to increment)
start = 0
end = 7

# 4. Loop and write with partitioning by bintime
for i in range(start, end):
    df_inc = df.withColumn("bintime", col("bintime") + lit(86400 * i)) \
        .withColumn("eventtime", col("eventtime") + lit(86400 * i))

    df_inc.write \
        .mode("append") \
        .partitionBy("usecasename","finalcubebintime","gran","roamertype","bintime") \
        .orc(output_path)

# Stop Spark
spark.stop()