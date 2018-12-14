import json
import re
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import udf, explode, split
from pyspark.sql.types import StringType
import pyspark.sql.functions as F

sc = SparkContext('local')
spark = SparkSession(sc)

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

# remove '[' and ']' in the data
# def remove_square_brackets(cast_ids):
#     return cast_ids[2: -1]

def remove_nonnumber(cast_id):
    result = re.sub('[^0-9]', '', cast_id)
    return result

# add "MOVIE_" in front of movie id
def get_movie_id(id):
    return "MOVIE_" + "%08d" % int(id)

# add "CAST_" in front of cast id
def get_cast_id(id):
    return "CAST_" + "%08d" % int(id)

# The serial number of each genre
genresDic = {'Mystery': 14, 'Romance': 8, 'History': 15, 'Family': 6, 'Fantasy': 10, 'Horror': 16, 'Crime': 0,
             'Drama': 7, 'Science Fiction': 4, 'Animation': 5, 'Music': 9, 'Adventure': 2, 'Foreign': 18, 'Action': 3,
             'Comedy': 1, 'Documentary': 17, 'War': 12, 'Thriller': 11, 'Western': 13}
# The serial number of each language
languageDic = {'en': 0, 'zh': 3, 'cn': 17, 'af': 9, 'vi': 20, 'is': 25, 'it': 6, 'xx': 22, 'id': 23, 'es': 2, 'ru': 12,
               'nl': 16, 'pt': 7, 'no': 18, 'nb': 21, 'th': 15, 'ro': 11, 'pl': 24, 'fr': 5, 'de': 1, 'da': 10,
               'fa': 19, 'hi': 13, 'ja': 4, 'he': 14, 'te': 26, 'ko': 8}


# # generate a standard id
# def get_id(movie_id):
#     return "%08d" % int(movie_id)


# Get the year of a date
# 2018-12-2 -> 2018
def get_year(date):
    if(date == None):
        return 1900
    return int(date.split("-")[0])


# transfer the language of the movie to an array
# for example:
# before: en
# after: [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
def get_language(language):
    language_array = [0] * (len(languageDic) + 1)
    # language_id = languageDic[language]
    language_id = languageDic.setdefault(language, 27)
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
        genre_id = genresDic[genre['name']]
        genres_array[genre_id] = 1
    return genres_array


get_id_udf = udf(get_id, StringType())
get_year__udf = udf(get_year, StringType())
get_language_udf = udf(get_language, StringType())
get_genres_udf = udf(get_genres, StringType())

# get_id_udf = udf(get_id, StringType())
get_casts_udf = udf(get_casts, StringType())
remove_nonnumber_udf = udf(remove_nonnumber, StringType())
get_movie_id_udf = udf(get_movie_id, StringType())
get_cast_id_udf = udf(get_cast_id, StringType())

castDataRaw1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option('quote',
                                                                                                      '"').option(
    'escape', '"').load(
    "/finalProjectData/tmdb_5000_credits.csv").select(
    "movie_id", "cast")

# transfer movie_id to standard format
castDataRaw2 = castDataRaw1.withColumn('mid', get_id_udf(castDataRaw1['movie_id']))

# parse cast to get all ids
castDataRaw2 = castDataRaw2.withColumn('cast_ids', get_casts_udf(castDataRaw2['cast']))

#explode cast ids
castDataRaw3 = castDataRaw2.withColumn('cast_id_1', explode(split(castDataRaw2['cast_ids'], ', ')))

#remove [ and ]
castDataRaw3 = castDataRaw3.withColumn('cast_id', remove_nonnumber_udf(castDataRaw3['cast_id_1']))
castDataRaw3 = castDataRaw3.select('movie_id', 'cast_id')

movieToCasts = castDataRaw3.groupBy("movie_id").agg(F.collect_list(F.col("cast_id")).alias("ids_2"))
castToMovies = castDataRaw3.groupBy("cast_id").agg(F.collect_list(F.col("movie_id")).alias("ids_2"))

movieToCasts = movieToCasts.withColumn('ids_1', get_movie_id_udf(movieToCasts['movie_id'])).select('ids_1', "ids_2")
castToMovies = castToMovies.withColumn('ids_1', get_cast_id_udf(castToMovies['cast_id'])).select('ids_1', "ids_2")

# append all castToMovies to the end of movieToCasts
castData = castToMovies.union(movieToCasts)

movieDataRaw1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option('quote',
                                                                                                       '"').option(
    'escape', '"').load(
    "/finalProjectData/tmdb_5000_movies.csv").select(
    "id", "original_language", "revenue", "title", "budget", "release_date", "genres")

# Transfer each attribute to standard format
movieDataRaw2 = movieDataRaw1.withColumn('mid', get_id_udf(movieDataRaw1['id']))
movieDataRaw2 = movieDataRaw2.withColumn('mlanguage', get_language_udf(movieDataRaw2['original_language']))
movieDataRaw2 = movieDataRaw2.withColumn('myear', get_year__udf(movieDataRaw2['release_date']))
movieDataRaw2 = movieDataRaw2.withColumn('mgenres', get_genres_udf(movieDataRaw2['genres']))

movieData = movieDataRaw2.select('mid', 'mlanguage', 'revenue', 'title', 'budget', 'myear', 'mgenres')

# castData.show()
# movieData.show()

#write movieData to elastic search
movieData.write.format("org.elasticsearch.spark.sql").option("es.nodes", "http://192.168.0.101:9200").mode("overwrite").save("bigdatafinal/movie")
#
# # write castData to elastic search
castData.write.format("org.elasticsearch.spark.sql").option("es.nodes", "http://192.168.0.101:9200").mode("overwrite").save("bigdatafinal/cast")
