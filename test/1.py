# -*- coding: utf-8 -*-
'''

'''
__author__ = 'Foxlora'
__time__ = '2020/10/7 22:08'
import os
import sys

from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf().setAppName('test')
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')

list1 = ["Hadoop","Spark","Hive","Spark"]
list2 = [("spark",2),("hadoop",6),("hadoop",4),("spark",6)]
rdd = sc.parallelize(list2)
a= rdd.mapValues(lambda x:(x,1)).reduceByKey(lambda a,b:(a[0]+ b[0],a[1]+b[1])).mapValues(lambda x:x[0]/x[1]).collect()
print(a)
