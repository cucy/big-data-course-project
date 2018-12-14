# big-data-course-project

1st step:
run main_data_processor in spark local to get cast_impresssion_data
/Users/wesley/important/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --master local[2] --name spark-local /Users/wesley/codes/bigdatafinal/data_process/main_data_processor.py

run main_data_processor in spark yarn to get cast_impresssion_data


/Users/wesley/important/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --master local[2] --jars elasticsearch-spark-20_2.11-6.5.2.jar /Users/wesley/codes/bigdatafinal/data_process/main_data_processor.py


hadoop fs -put /Users/wesley/codes/python/test/tmdb_5000_movies.csv /finalProjectData/

before running
hadoop fs -ls /finalProjectData/

hadoop fs -rm -r /finalProjectData/result
hadoop fs -rm -r /finalProjectData/tmdb_5000_movies.csv
hadoop fs -rm -r /finalProjectData/tmdb_5000_credits.csv

hadoop fs -put /Users/wesley/codes/python/test/tmdb_5000_movies.csv /finalProjectData/
hadoop fs -put /Users/wesley/codes/python/test/tmdb_5000_credits.csv /finalProjectData/

hadoop fs -get /finalProjectData/result/data_impression.csv /Users/wesley/codes/python/test/
