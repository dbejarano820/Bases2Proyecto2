
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (DateType, IntegerType, FloatType, StructField,
                               StructType, TimestampType, StringType)
import pyspark.sql.functions as f
import random
from collections import Counter

from pyspark.sql.functions import array_join, collect_list

spark = SparkSession.builder.appName("Ejemplo 1 bases2").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


#RAW VIBES
'''
dataframe = spark.read \
    .format("parquet") \
    .load("./dest/raw") 

filtered_df = dataframe.select(f.split(dataframe.value,";")) \
              .rdd.flatMap(lambda x: x) \
              .toDF(schema=["_id","artist_name","track_name", "release_date", "genre", "lyrics", "len"])
'''

#-----------------------------------------------------------------------------------
filtered_df = spark.read.options(header='True', inferSchema='True', delimiter=';') \
    .csv("pruebas/lyrics.csv")
#filtered_df.show()
#-----------------------------------------------------------------------------------

# #Additional
'''
dataframe = spark.read \
    .format("parquet") \
    .load("./dest/additional") 

schema = StructType().add('word', StringType(), False).add('type', StringType(), False)
filtered_Df_log = dataframe.select(f.from_json('value', schema).alias('temp')).select('temp.*')

filtered_Df_log.createOrReplaceTempView("additionalvibes")
'''

#-----------------------------------------------------------------------------------
filtered_Df_log = spark.read.options(header='True', inferSchema='True', delimiter=';') \
    .csv("pruebas/sentiment.csv")
#filtered_Df_log.show()
filtered_Df_log.createOrReplaceTempView("additionalvibes")
#-----------------------------------------------------------------------------------

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
analyzed_df = filtered_df.rdd.map(lambda x: analyze(x)).toDF(schema=["song", "year", "sentiment", "genre", "theme"])
analyzed_df.createOrReplaceTempView("analyzed_songs")
analyzed_df.show()

# a) ratio entre positivo y negativo por año
print("+++SENTIMIENTO POR AÑO+++")
def calculateValuesA(row):
    total =  row.positive + row.negative
    ratio = row.positive / total
    return (row.year, row.positive, row.negative, total, ratio)

sentiment_by_year = spark.sql("select year," +\
        "sum(case when sentiment= 'positive' or sentiment= 'neutral' then 1 else 0 end) AS positive," +\
        "sum(case when sentiment= 'negative' then 1 else 0 end) AS negative " +\
        "from analyzed_songs group by year")
topicA = sentiment_by_year.rdd.map(lambda x: calculateValuesA(x)).toDF(schema=["year", "positive", "negative", "total", "ratio"])
topicA = topicA.withColumn('question', f.lit('a'))
topicA.show()
#enviar a processed_vibes




# b) sentimiento predominante por genero musical por año
print("+++SENTIMIENTO POR GENERO POR AÑO+++")
def calculateValuesB(row):
    total =  row.positive + row.negative
    ratio = row.positive / total
    return (row.genre, row.year, row.positive, row.negative, total, ratio)

sentiment_by_genre_year = spark.sql("select genre, year," +\
        "sum(case when sentiment= 'positive' or sentiment= 'neutral' then 1 else 0 end) AS positive," +\
        "sum(case when sentiment= 'negative' then 1 else 0 end) AS negative " +\
        "from analyzed_songs group by genre, year")
topicB = sentiment_by_genre_year.rdd.map(lambda x: calculateValuesB(x)).toDF(schema=["genre", "year", "positive", "negative", "total", "ratio"])
topicB = topicB.withColumn('question', f.lit('b'))
topicB.show()
#enviar a processed_vibes




# c) temas centrales por año 
print("+++TEMAS POR AÑO+++")
def getThemePerYear(row):
    split_themes = row.themes.split(",")
    themes = getMainThemes(split_themes)
    return(row.year, themes)

#themes_per_year = spark.sql("select year, GROUP_CONCAT(theme) theme from analyzed_songs group by year")
themes_per_year = analyzed_df\
    .orderBy('year', ascending=False)\
    .groupBy('year')\
    .agg(
        array_join(
            collect_list('theme'),
            delimiter=',',
        ).alias('themes')
    )
#themes_per_year.show(truncate=False)
topicC = themes_per_year.rdd.map(lambda x: getThemePerYear(x)).toDF(schema=["year", "themes"])
topicC = topicC.withColumn('question', f.lit('c'))
topicC.show(truncate=False)
#enviar a processed_vibes


json_df = topicA.select(f.col("year"), f.to_json(f.struct("*"))).toDF("key", "value")
query = json_df \
  .selectExpr("CAST(value AS STRING)") \
  .write \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "10.0.0.2:9092") \
  .option("topic", "processed_vibes") \
  .save()

json_df = topicB.select(f.col("genre"), f.to_json(f.struct("*"))).toDF("key", "value")
query = json_df \
  .selectExpr("CAST(value AS STRING)") \
  .write \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "10.0.0.2:9092") \
  .option("topic", "processed_vibes") \
  .save()

json_df = topicC.select(f.col("year"), f.to_json(f.struct("*"))).toDF("key", "value")
query = json_df \
  .selectExpr("CAST(value AS STRING)") \
  .write \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "10.0.0.2:9092") \
  .option("topic", "processed_vibes") \
  .save()


