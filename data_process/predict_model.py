from __future__ import print_function
from pyspark import SparkContext, SQLContext
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.ml.regression import LinearRegression
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.stat import Statistics
import json, ast
import configparser
import math


save = False
#/Users/wesley/codes/bigdatafinal/data/twitter_data.json
#hadoop fs -put /Users/wesley/codes/bigdatafinal/data/twitter_data.json /finalProjectData/
#hadoop fs -put /Users/wesley/codes/bigdatafinal/data/model_config.cfg /finalProjectData/

# twitterFile = "file:///Users/jpliu/PycharmProjects/BigDataFinalProject/venv/resources/twitter_data.json"
# movieFile = "file:///Users/jpliu/PycharmProjects/BigDataFinalProject/venv/resources/data_impression.csv"
# model_config = "/Users/jpliu/PycharmProjects/BigDataFinalProject/venv/resources/model_config.cfg"

twitterFile = "/finalProjectData/twitter_data.json"
movieFile = "/finalProjectData/result/data_impression.csv"
outputFile = "/Users/wesley/codes/machine.txt"

sc = SparkContext('local', 'predict_model')
sqlContext = SQLContext(sc)

# build the machine learning model


def parse_twitter_statistic(line):
    line = json.loads(line)
    # movie_id = line.pop('movie_id')
    line.pop('oa_screen_name')
    line.pop('movie_name')
    movie_id = line.pop('movie_id')
    line = line.items()

    line = sorted(line)
    result = []
    for key, value in line:
        result.append(value)

    return (movie_id, result)



# parse the movie data
#   get the movie year between 2007 - 2017 and revenue between 5000000 and 1000000000000
def parse_movie(line):
    line = line.asDict()
    if (int(line['myear']) >= 2007 and int(line['myear']) <= 2017 and int(line['revenue']) >= 5000000 and int(line['revenue']) <= 1000000000000):
        return True
    return False


def parse_movie_id(line):
    values = [float(x) for x in line]
    return (int(values[0]), values[1:])

def parseMovie(line):
    print(line)

# reuturn movie rdd
def movie_data_process():
    data = sqlContext.read.format('csv').option("header", "true").\
        load(movieFile).\
        select('mid','revenue','budget','cast_impression','myear','mgenres','mlanguage')

    movieRdd = data.rdd
    movieRdd = movieRdd.filter(parse_movie)
    movieRdd = movieRdd.map(lambda x : [x.mid] + [x.revenue] + [x.budget] + [x.cast_impression] + [x.myear] + ast.literal_eval(x.mgenres) +
                                     ast.literal_eval(x.mlanguage))
    movieRdd = movieRdd.map(parse_movie_id)

    return movieRdd

# return twitter rdd
def twitter_data_process():
    twitter = sc.textFile(twitterFile).map(parse_twitter_statistic)
    return twitter

# mapper from (id, [revenue features]) to LabeledPoint
def tuple_to_LB(line, strip = False):
    twitter_features = line[1][0]
    tmdb_features = line[1][1]
    revenue = line[1][1][0]
    if not strip:
        return LabeledPoint(revenue, tmdb_features + twitter_features)
    else:
        return LabeledPoint(revenue, tmdb_features)


def get_cata_dict():
    res = {}
    for x in range(4,47):
        res[x] = 2
    return res

# model factory
def model_factory(training_data, categorical_features_info={}):
    model = None
    maxDepth = 4
    maxBins = 64
    numTrees = 80
    model = RandomForest.trainRegressor(training_data, categoricalFeaturesInfo=categorical_features_info,
                                            numTrees=numTrees, featureSubsetStrategy="auto",
                                            impurity='variance', maxDepth=maxDepth, maxBins=maxBins)
    return model

#evaluate the model we build by the training_data
def evaluation(model, test_data, f):
    predictions = model.predict(test_data.map(lambda x: x.features))
    gold = test_data.map(lambda x : x.label)
    labelsAndPredictions = test_data.map(lambda lp: lp.label).zip(predictions)
    test_percentage_err_mean = labelsAndPredictions.map(lambda lp: abs(lp[0] - lp[1]) / lp[0]).sum() / float(labelsAndPredictions.count())
    test_error_sqaure_mean= labelsAndPredictions.map(lambda lp: (lp[0] - lp[1]) * (lp[0] - lp[1])).sum() / float(labelsAndPredictions.count())
    test_error_sqaure_mean = math.sqrt(test_error_sqaure_mean) / float(labelsAndPredictions.count())
    f.write("evaluating the section: \n")
    f.write("THe pearson corrolation equals:\n")
    f.write(str(Statistics.corr(labelsAndPredictions, method="pearson")) + '\n')
    f.write('Test Mean Squared Error = ' + str(test_error_sqaure_mean) + '\n')
    f.write('Test Mean Precentage Error = ' + str(test_percentage_err_mean) + '\n')

if __name__ == '__main__':
    # process data rdd and then join data togerther with movie id
    movieRdd = movie_data_process()
    twitter = twitter_data_process()
    parsed_data = twitter.join(movieRdd)
    parsed_data = parsed_data.map(lambda x : tuple_to_LB(x))
    training_data, test_data = parsed_data.randomSplit([0.7, 0.3])
    f = open(outputFile, 'w')
    model = model_factory(training_data, get_cata_dict())
    evaluation(model, test_data, f)
