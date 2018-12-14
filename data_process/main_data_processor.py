from dateutil.parser import parse

import pyspark.sql.functions as F
import json
import time
import re
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, split
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# The serial number of each genre
genresDic = {'Mystery': 14, 'Romance': 8, 'History': 15, 'Family': 6, 'Fantasy': 10, 'Horror': 16, 'Crime': 0,
             'Drama': 7, 'Science Fiction': 4, 'Animation': 5, 'Music': 9, 'Adventure': 2, 'Foreign': 18, 'Action': 3,
             'Comedy': 1, 'Documentary': 17, 'War': 12, 'Thriller': 11, 'Western': 13, 'error_data': 99}
# The serial number of each language
languageDic = {'en': 0, 'zh': 3, 'cn': 17, 'af': 9, 'vi': 20, 'is': 25, 'it': 6, 'xx': 22, 'id': 23, 'es': 2, 'ru': 12,
               'nl': 16, 'pt': 7, 'no': 18, 'nb': 21, 'th': 15, 'ro': 11, 'pl': 24, 'fr': 5, 'de': 1, 'da': 10,
               'fa': 19, 'hi': 13, 'ja': 4, 'he': 14, 'te': 26, 'ko': 8}


def square_rev(n):
    return n*n


# parse all ids from cast and return a vector
def get_casts(casts):
    casts_json = json.loads(casts)
    castids = []
    for cast in casts_json:
        castid = cast['cast_id']
        castids.append(castid)
    return castids


# generate a standard id
def get_id(movie_id):
    return "%08d" % int(movie_id)

def remove_nonnumber(cast_id):
    result = re.sub('[^0-9]', '', cast_id)
    return result

def get_movie_id(id):
    return "%08d" % int(id)

# add "CAST_" in front of cast id
def get_cast_id(id):
    return "CAST_" + "%08d" % int(id)


# check whether this is a valid date formated in 12/31/19
# def is_valid_date(str):
#     try:
#         time.strptime(str, "%m/%d/%y")
#         return True
#     except:
#         return False

def is_date(string):
    try:
        parse(string)
        return True
    except BaseException:
        return False

# generate a standard id
def get_id(movie_id):
    return "%08d" % int(movie_id)


# Get the year of a date
# 2018-12-2 -> 2018
def get_year(date):
    year = int(date.split("-")[2])
    # if(year >= 0 and year <= 18):
    #     year = 2000 + year % 100
    # else:
    #     year = 1900 + year % 100
    return year


# transfer the language of the movie to an array
# for example:
# before: en
# after: [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
def get_language(language):
    language_array = [0] * len(languageDic)
    language_id = languageDic[language]
    language_array[language_id] = 1
    return language_array


# transfer the genre of the movie to an array
# for example:
# before: [{"id": 80, "name": "Crime"}, {"id": 35, "name": "Comedy"}]
# after: [1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
def get_genres(genres):
    genres_json = json.loads(genres)
    genres_array = [0] * len(genresDic)
    for genre in genres_json:
        genre_id = genresDic.get(genre['name'], 0)
        genres_array[genre_id] = 1
    return genres_array


get_id_udf = udf(get_id, StringType())
get_year__udf = udf(get_year, StringType())
get_language_udf = udf(get_language, StringType())
get_genres_udf = udf(get_genres, StringType())
get_casts_udf = udf(get_casts, StringType())
remove_nonnumber_udf = udf(remove_nonnumber, StringType())
get_movie_id_udf = udf(get_movie_id, StringType())
get_cast_id_udf = udf(get_cast_id, StringType())


# used to clean cast datas
def clean_cast_data(line):
    dict = line.asDict()
    # print(dict)
    if('movie_id' not in dict):
        return False

    if('cast' not in dict):
        return False

    return True

#used to clean movie datas
def clean_movie_data(line):

    dict = line.asDict()
    # print(dict)

    if('id' not in dict or dict['id'].isdigit() == False):
        return False
        # return Row(id = '9999999', original_language = 'en', revenue = 0, title = "error_data", budget = 0, release_date = '0000-00-00', genres = '[{"id": 99, "name": "error_data"}]')

    # original_language is in languageDict
    if('original_language' not in dict or languageDic.get(dict['original_language'], 99) == 99):
        return False

    # revenue is digit
    if('revenue' not in dict or dict['id'].isdigit() == False):
        return False

    # title, doesn't matter

    # budget is digit
    if('budget' not in dict or dict['budget'].isdigit() == False):
        return False

    # release_date, is a date
    if('release_date' not in dict or is_date(dict['release_date']) == False):
        return False

    # genres, is in genresDic
    if('genres' not in dict):
        return False

    return True


#process cast data
def cast_data_process(spark):

    castDataRaw1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option('quote',
                                                                                                          '"').option(
        'escape', '"').load(
        "/finalProjectData/tmdb_5000_credits.csv").select(
        "movie_id", "cast")

    temp = castDataRaw1.rdd
    temp = temp.filter(clean_cast_data)
    castDataRaw1 = spark.createDataFrame(temp)

    # transfer movie_id to standard format
    castDataRaw2 = castDataRaw1.withColumn('mid', get_id_udf(castDataRaw1['movie_id']))

    # parse cast to get all ids
    castDataRaw2 = castDataRaw2.withColumn('cast_ids', get_casts_udf(castDataRaw2['cast']))

    #explode cast ids
    castDataRaw3 = castDataRaw2.withColumn('cast_id_1', explode(split(castDataRaw2['cast_ids'], ', ')))

    #remove [ and ]
    castDataRaw3 = castDataRaw3.withColumn('cast_id', remove_nonnumber_udf(castDataRaw3['cast_id_1']))
    castDataRaw3 = castDataRaw3.select('movie_id', 'cast_id')

    result = castDataRaw3.withColumn('mid2', get_movie_id_udf(castDataRaw3['movie_id']))
    result = result.select('mid2', 'cast_id')

    return result


# process movie's raw data, return a ready rdd object
def movie_data_process(spark):
    # Get all attributes we need from the original csv file
    # This uses file in hadoop in order to make it scalable
    movieDataRaw1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option('quote',
                                                                                                           '"').option(
        'escape', '"').load(
        "file:///Users/wesley/codes/python/test/tmdb_5000_movies.csv").select(
        "id", "original_language", "revenue", "title", "budget", "release_date", "genres")

    # movieDataRaw1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option('quote',
    #                                                                                                        '"').option(
    #     'escape', '"').load(
    #     "/finalProjectData/tmdb_5000_movies.csv").select(
    #     "id", "original_language", "revenue", "title", "budget", "release_date", "genres")
    # movieDataRaw1.show()
    temp = movieDataRaw1.rdd
    temp = temp.filter(clean_movie_data)

    movieDataRaw1 = spark.createDataFrame(temp)

    # Transfer each attribute to standard format
    movieDataRaw2 = movieDataRaw1.withColumn('mid', get_id_udf(movieDataRaw1['id']))
    movieDataRaw2 = movieDataRaw2.withColumn('mlanguage', get_language_udf(movieDataRaw2['original_language']))
    movieDataRaw2 = movieDataRaw2.withColumn('myear', get_year__udf(movieDataRaw2['release_date']))
    movieDataRaw2 = movieDataRaw2.withColumn('mgenres', get_genres_udf(movieDataRaw2['genres']))

    movieData = movieDataRaw2.select('mid', 'mlanguage', 'revenue', 'title', 'budget', 'myear', 'mgenres')
    return movieData


if __name__ == '__main__':
    # spark = SparkSession.builder.appName("project").getOrCreate()
    SparkContext.getOrCreate().stop()
    sc = SparkContext('local')
    spark = SparkSession(sc)

    #get movieData and cast data
    movieData = movie_data_process(spark)
    castData = cast_data_process(spark)

    #write to csv
    # movieData.toPandas().to_csv('data_movieInfo.csv')
    # castData.toPandas().to_csv('data_movie_cast.csv')

    movieData.write.option("header", "true").format("csv").save("/finalProjectData/result/data_movieInfo.csv")
    castData.write.option("header", "true").format("csv").save("/finalProjectData/result/data_movie_cast.csv")

    # test = castData.rdd
    # test.saveAsTextFile("/finalProjectData/1.txt")

    #read from csv
    movie_cast = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option('quote',
                                                                                                        '"').option(
        'escape', '"').load(
        "/finalProjectData/result/data_movie_cast.csv").select(
        "mid2", "cast_id")

    movieInfo = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option('quote',
                                                                                                       '"').option(
        'escape', '"').load(
        "/finalProjectData/result/data_movieInfo.csv").select(
        "mid", "mlanguage", "revenue", "title", "budget", "myear", "mgenres")

    # int the following code, we calculate the cast_impression
    # This is the schema of movie_cast_renue
    # | -- mid2: integer(nullable=true)
    # | -- cast_id: integer(nullable=true)
    # | -- mid: integer(nullable=true)
    # | -- mlanguage: string(nullable=true)
    # | -- revenue: integer(nullable=true)
    # | -- title: string(nullable=true)
    # | -- budget: integer(nullable=true)
    # | -- myear: integer(nullable=true)
    # | -- mgenres: string(nullable=true)
    movie_cast_renue = movie_cast.join(movieInfo, movie_cast.mid2 == movieInfo.mid)

    # | -- cast_id: integer(nullable=true)
    # | -- avg(revenue): double(nullable=true)
    average_cast_revenue = movie_cast_renue.groupBy("cast_id").agg(F.avg("revenue"))

    # | -- cid: integer(nullable=true)
    # | -- avg(revenue): double(nullable=true)
    average_cast_revenue = average_cast_revenue.withColumnRenamed("cast_id", "cid")


    movie_cast_average_revenue = movie_cast_renue.join(average_cast_revenue, average_cast_revenue.cid == movie_cast_renue.cast_id)

    square_rev_udf = F.udf(square_rev, StringType())
    movie_cast_average_revenue = movie_cast_average_revenue.withColumn('square_rev', square_rev_udf(movie_cast_average_revenue['avg(revenue)']))
    movie_cast_average_revenue = movie_cast_average_revenue.groupBy('mid').agg(F.sum('square_rev'))
    movie_cast_average_revenue = movie_cast_average_revenue.withColumn('cast_impression', F.sqrt(movie_cast_average_revenue['sum(square_rev)']))
    movie_cast_average_revenue = movie_cast_average_revenue.withColumnRenamed("mid", "mid2")
    movie_cast_average_revenue.printSchema()

    result = movie_cast_average_revenue.join(movieInfo, movieInfo.mid == movie_cast_average_revenue.mid2)
    result = result.select("mid", "mlanguage", "revenue", "title", "budget", "myear", "mgenres", "cast_impression")
    # result.toPandas().to_csv('data_impression.csv')
    result.write.option("header", "true").format("csv").save("/finalProjectData/result/data_impression.csv")
    # result.show()

    spark.stop()
