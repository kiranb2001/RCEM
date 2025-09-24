from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, date_add

# Define the Paths :
input_path =  "/user/rcemojo/output/tripmetrics/overalltripstatus=closed"
output_path = "/user/rcemojo/output/ssisqi_performance/tripmetrics/overalltripstatus=closed"

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("Tripmetrics_Extrapolation") \
    .getOrCreate()

# 2. Read ORC file
df = spark.read.orc(input_path)

# 3. Define start and end (number of days to increment)
start = 15
end = 15

# 4. Loop and write with partitioning by bintime
for i in range(start, end+1):
    df_inc = df.withColumn("bintime", col("bintime") + lit(86400 * i)) \
               .withColumn("tripstarttime", col("tripstarttime") + lit(86400000 * i)) \
               .withColumn("tripendtime", col("tripendtime") + lit(86400000 * i)) \
               .withColumn("globaltripid", col("globaltripid") + lit(86400000 * i)) \
               .withColumn("countrytripid", col("countrytripid") + lit(86400000 * i)) \
               .withColumn("networktripid", col("networktripid") + lit(86400000 * i))

    df_inc.write \
        .mode("append") \
        .partitionBy("imsihash","bintime","triptype") \
        .orc(output_path)

# Stop Spark
spark.stop()