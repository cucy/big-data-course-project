from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

if __name__ == '__main__':
    SparkContext.getOrCreate().stop()
    sc = SparkContext('local')
    spark = SparkSession(sc)
    # data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option('quote',
    #                                                                                        '"').option(
    #     'escape', '"').load(
    #     "file:///Users/wesley/codes/python/pyspark_project/project/data_impression.csv").select(
    #     "mid", "mlanguage", "revenue", "title", "budget", "myear", "mgenres", "cast_impression")

    data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option('quote',
                                                                                           '"').option(
        'escape', '"').load(
        "/finalProjectData/result/data_impression.csv").select(
        "mid", "mlanguage", "revenue", "title", "budget", "myear", "mgenres", "cast_impression")

    # data.show()
    # data.toPandas().to_csv('data_impression.csv')

    data.write.format("org.elasticsearch.spark.sql").option("es.nodes", "http://192.168.0.101:9200").mode(
        "overwrite").save("testtest1812141428/2")
