# -*- coding: utf-8 -*-
'''

'''
__author__ = 'Foxlora'
__time__ = '2020/10/10 18:28'


from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
conf = SparkConf().setAppName('test')
sc = SparkContext(conf=conf)


