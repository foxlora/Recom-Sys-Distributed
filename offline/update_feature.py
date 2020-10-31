

from pyspark.sql import HiveContext
from offline import SparkSessionBase
from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf().setAppName('test')
sc = SparkContext(conf=conf)

hive_context = HiveContext(sc)
hive_context.sql('use default')

hive_context.sql('show * from ratings').show()