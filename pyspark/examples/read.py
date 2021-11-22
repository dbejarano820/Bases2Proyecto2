
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (DateType, IntegerType, FloatType, StructField,
                               StructType, TimestampType, StringType)
import pyspark.sql.functions as f

spark = SparkSession.builder.appName("Ejemplo 1 bases2").getOrCreate()

# csv_schema = StructType([StructField('_id', StringType(), True),
#                          StructField('artist_name', StringType(), True),
#                          StructField('track_name', StringType(), True),
#                          StructField('release_date', StringType(), True),
#                          StructField('genre', StringType(), True),
#                          StructField('lyrics', StringType(), True),
#                          StructField('len', StringType(), True),
#                          ])


# dataframe = spark.read.parquet("./dest") \
#     .selectExpr("CAST(value AS STRING)") \

dataframe = spark.read \
    .format("parquet") \
    .load("./dest")

filtered_Df = dataframe.select(f.split(dataframe.value,",")) \
              .rdd.flatMap(lambda x: x) \
              .toDF(schema=["_id","artist_name","track_name", "release_date", "genre", "lyrics", "len"])

dataframe.createOrReplaceTempView("rawvibes")

queryConSparkSQL = spark.sql("""
  select *
  from rawvibes
""")

queryConSparkSQL.show()

dataframe.printSchema()

filtered_Df.printSchema()
