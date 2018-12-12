from project.cast_data_processor import cast_data_process
from project.movie_data_processor import movie_data_process
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("project").getOrCreate()
    movieData = movie_data_process(spark)
    castData = cast_data_process(spark)
    spark.stop()
