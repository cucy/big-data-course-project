import json
import re

from cast_data_cleaner import clean_cast_data
from pyspark.sql.functions import udf, explode, split
from pyspark.sql.types import StringType

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

get_id_udf = udf(get_id, StringType())
get_casts_udf = udf(get_casts, StringType())
remove_nonnumber_udf = udf(remove_nonnumber, StringType())
get_movie_id_udf = udf(get_movie_id, StringType())
get_cast_id_udf = udf(get_cast_id, StringType())



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
