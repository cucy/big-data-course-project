# from project.cast_data_processor import cast_data_process
# from project.movie_data_processor import movie_data_process

from cast_data_processor import cast_data_process
from movie_data_processor import movie_data_process
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
# from pyspark.context import SparkContext
# from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

# if __name__ == '__main__':
#     sc = SparkContext('local')
#     spark = SparkSession(sc)
#     # spark = SparkSession.builder.appName("project").getOrCreate()
#     movieData = movie_data_process(spark)
#     castData = cast_data_process(spark)
#
#     #write movieData to elastic search
#     movieData.write.format("org.elasticsearch.spark.sql").option("es.nodes", "http://192.168.0.101:9200").mode("overwrite").save("bigdatafinal/movie")
#
#     # write castData to elastic search
#     # castData.write.format("org.elasticsearch.spark.sql").option("es.nodes", "http://192.168.0.101:9200").mode("overwrite").save("bigdatafinal/cast")
#
#     # movieData.show()
#     # castData.show()
#
#
#     spark.stop()
sc = SparkContext('local')
spark = SparkSession(sc)
# spark = SparkSession.builder.appName("project").getOrCreate()
movieData = movie_data_process(spark)
castData = cast_data_process(spark)

# movieData.show()
# castData.show()

#write movieData to elastic search
movieData.write.format("org.elasticsearch.spark.sql").option("es.nodes", "http://192.168.0.101:9200").mode("overwrite").save("bigdatafinal/movie")

# write castData to elastic search
castData.write.format("org.elasticsearch.spark.sql").option("es.nodes", "http://192.168.0.101:9200").mode("overwrite").save("bigdatafinal/cast")

# movieData.show()
# castData.show()


# spark.stop()
