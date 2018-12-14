from cast_data_processor import cast_data_process
from movie_data_processor import movie_data_process
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

def square_rev(n):
    return n*n

if __name__ == '__main__':
    # spark = SparkSession.builder.appName("project").getOrCreate()
    SparkContext.getOrCreate().stop()
    sc = SparkContext('local')
    spark = SparkSession(sc)

    #get movieData and cast data
    movieData = movie_data_process(spark)
    castData = cast_data_process(spark)

    #write to csv
    movieData.toPandas().to_csv('data_movieInfo.csv')
    castData.toPandas().to_csv('data_movie_cast.csv')

    #read from csv
    movie_cast = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option('quote',
                                                                                                        '"').option(
        'escape', '"').load(
        "file:///Users/wesley/codes/python/pyspark_project/project/data_movie_cast.csv").select(
        "mid2", "cast_id")

    movieInfo = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option('quote',
                                                                                                       '"').option(
        'escape', '"').load(
        "file:///Users/wesley/codes/python/pyspark_project/project/data_movieInfo.csv").select(
        "mid", "mlanguage", "revenue", "title", "budget", "myear", "mgenres")

    # int the following code, we calculate the cast_impression

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
    result.toPandas().to_csv('data_impression.csv')

    spark.stop()
