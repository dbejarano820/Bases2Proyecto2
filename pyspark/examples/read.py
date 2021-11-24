
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (DateType, IntegerType, FloatType, StructField,
                               StructType, TimestampType, StringType)
import pyspark.sql.functions as f

spark = SparkSession.builder.appName("Ejemplo 1 bases2").getOrCreate()

schema = StructType

#RAW VIBES
dataframe = spark.read \
    .format("parquet") \
    .load("./dest/raw") 

dataframe.show()

dataframe.printSchema()

filtered_df = dataframe.select(f.split(dataframe.value,";")) \
              .rdd.flatMap(lambda x: x) \
              .toDF(schema=["_id","artist_name","track_name", "release_date", "genre", "lyrics", "len"])


# filter.printSchema()
filtered_df.createOrReplaceTempView("rawvibes")


queryConSparkSQL = spark.sql("""
  select *
  from rawvibes
""")

queryConSparkSQL.show()





# # #Additional
# dataframe = spark.read \
#     .format("parquet") \
#     .load("./dest/additional") 

# schema = StructType().add('word', StringType(), False).add('type', StringType(), False)
# filtered_Df_log = dataframe.select(f.from_json('value', schema).alias('temp')).select('temp.*')

# filtered_Df_log.createOrReplaceTempView("additionalvibes")

# queryConSparkSQL = spark.sql("""
#   select *
#   from additionalvibes
# """)

# queryConSparkSQL.show()