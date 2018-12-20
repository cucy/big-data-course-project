# Big Data Course Final Project

# Versions:
- hadoop 2.9.2
- Spark 2.4.0
- elasticsearch 6.5.2
- Kibana 6.5.2
- python 3.7.1

# How to run 
## use Azkaban
upload big-data-course-project/Azkaban/upload this.zip to Azkaban and run this task

## OR use command line
## 1. start environment:
1) Hadoop:  
/Users/wesley/important/hadoop-2.9.2/sbin/start-dfs.sh  
/Users/wesley/important/hadoop-2.9.2/sbin/start-yarn.sh  

2) Azkaban:
/Users/wesley/important/azkaban-solo-server-0.1.0-SNAPSHOT/bin/azkaban-solo-start.sh

## 2. remove all result datas and source datas in hadoop(if exists)
  
hadoop fs -rm -r /finalProjectData/result  

hadoop fs -rm -r /finalProjectData/tmdb_5000_movies.csv  

hadoop fs -rm -r /finalProjectData/tmdb_5000_credits.csv

## 3. add source data to /finalProjectData/  

hadoop fs -put /Users/wesley/codes/python/test/tmdb_5000_movies.csv /finalProjectData/  

hadoop fs -put /Users/wesley/codes/python/test/tmdb_5000_credits.csv /finalProjectData/

## 4. run main_data_processor in spark local to get cast_impresssion_data  

/Users/wesley/important/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --master local[2] --name spark-local  

/Users/wesley/codes/bigdatafinal/data_process/main_data_processor.py

## 5. run predict to do machine learning
/Users/wesley/important/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --master local[2] --name spark-local2 /Users/wesley/codes/bigdatafinal/data_process/predict_model.py
