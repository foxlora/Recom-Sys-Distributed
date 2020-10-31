
# -*- coding: utf-8 -*-
'''

'''
__author__ = 'Foxlora'
__time__ = '2020/10/30 9:33'

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import  SparkSession
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
import os
conf = SparkConf().setAppName('test3')
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')

spark = SparkSession.builder.getOrCreate()

df_moives = spark.read.csv("file:///root/Recom-Sys-Distributed/data/movies.csv",header="true")
df_ratings = spark.read.csv("file:///root/Recom-Sys-Distributed/data/ratings.csv",header="true")

# tokenizer = Tokenizer(inputCol="genres", outputCol="words")
# wordsData = tokenizer.transform(df_moives)
#
# hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
# featurizedData = hashingTF.transform(wordsData)
# # alternatively, CountVectorizer can also be used to get term frequency vectors
#
# idf = IDF(inputCol="rawFeatures", outputCol="features")
# idfModel = idf.fit(featurizedData)
# rescaledData = idfModel.transform(featurizedData)
#
# rescaledData.select("movieId", "features").show()
df_moives.mapInPandas