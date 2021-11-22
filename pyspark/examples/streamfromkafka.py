from datetime import datetime
from typing import cast
from pyspark.sql import DataFrame, SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import (DateType, IntegerType, FloatType, StructField,
                               StructType, TimestampType, StringType)
import pyspark.sql.functions as f

spark = SparkSession.builder.appName("Integracion con Kafka").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "10.0.0.2:9092") \
  .option("subscribe", "raw_vibes") \
  .load() 

filtered_df = df.selectExpr("CAST(value AS STRING)")

query = filtered_df.writeStream \
  .format("parquet") \
  .outputMode("append") \
  .option("path", "./dest") \
  .option("checkpointLocation", "./checkpoints") \
  .start()
query.awaitTermination()

# antes de hacer start debe haber algun output streaming operation
ssc = StreamingContext(spark.sparkContext, 1)
ssc.start()
ssc.awaitTermination()
