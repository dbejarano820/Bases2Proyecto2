
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (DateType, IntegerType, FloatType, StructField,
                               StructType, TimestampType, StringType)
import pyspark.sql.functions as f
import random
from collections import Counter

spark = SparkSession.builder.appName("Ejemplo 1 bases2").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#RAW VIBES
dataframe = spark.read \
    .format("parquet") \
    .load("./dest/raw") 

filtered_df = dataframe.select(f.split(dataframe.value,";")) \
              .rdd.flatMap(lambda x: x) \
              .toDF(schema=["_id","artist_name","track_name", "release_date", "genre", "lyrics", "len"])

# #Additional
dataframe = spark.read \
    .format("parquet") \
    .load("./dest/additional") 

schema = StructType().add('word', StringType(), False).add('type', StringType(), False)
filtered_Df_log = dataframe.select(f.from_json('value', schema).alias('temp')).select('temp.*')

filtered_Df_log.createOrReplaceTempView("additionalvibes")

### ANALYSIS ####



def getLyricsPercentage(lyrics):
    arr = []
    split_lyrics = lyrics.split(" ")
    for x in split_lyrics:
        a = random.randint(1,100)
        if (a < 100):  #Percetmage sample of lyrics
            arr.append(x)
    
    return arr


def getRowSentiment(lyrics):

    positives = 0
    negatives = 0

    for x in lyrics:

        sent = additional_dict.get(x)

        if sent == "negative": 
            negatives += 1
        elif sent == "positive":
            positives += 1
    
    if positives > negatives:
        return "positive"
    elif negatives > positives:
        return "negative"
    else: 
        return "neutral"


def getMainThemes(lyrics):
    wordCounter = dict()
    for i in lyrics:
        wordCounter[i] = wordCounter.get(i, 0) + 1

    themes = sorted(wordCounter, key=wordCounter.get, reverse=True)[:3]

    if(len(themes) < 3):
        themes.append("---")

    if(len(themes) < 3):
        themes.append("---")

    return themes[0]+","+themes[1]+","+themes[2]


def analyze(rec):
    arr_lyrics = getLyricsPercentage(rec.lyrics)
    sentiment = getRowSentiment(arr_lyrics)
    themes = getMainThemes(arr_lyrics)
    return (rec.track_name, rec.release_date, sentiment, rec.genre, themes)
    #Cancion, año, sentimiento, genero, tema central


additional_dict = {row['word']:row['type'] for row in filtered_Df_log.collect()}
final_df = filtered_df.rdd.map(lambda x: analyze(x)).toDF(schema=["song", "year", "sentiment", "genre", "theme"])

json_df = final_df.select(f.col("song"), f.to_json(f.struct("*"))).toDF("key", "value")

query = json_df \
  .selectExpr("CAST(value AS STRING)") \
  .write \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "10.0.0.2:9092") \
  .option("topic", "processed_vibes") \
  .save()




