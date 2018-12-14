import json
import re

from pyspark.sql.functions import udf, explode, split
from pyspark.sql.types import StringType
import pyspark.sql.functions as F


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

get_id_udf = udf(get_id, StringType())
get_casts_udf = udf(get_casts, StringType())
remove_nonnumber_udf = udf(remove_nonnumber, StringType())
get_movie_id_udf = udf(get_movie_id, StringType())
get_cast_id_udf = udf(get_cast_id, StringType())



def cast_data_process(spark):
    # castDataRaw1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option('quote',
    #                                                                                                       '"').option(
    #     'escape', '"').load(
    #     "file:///Users/wesley/codes/RBDA_Project/Data/tmdb_5000_credits.csv").select(
    #     "movie_id", "cast")

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
    result = castToMovies.union(movieToCasts)

    return result