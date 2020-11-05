

from pyspark.sql import HiveContext
from offline import SparkSessionBase
from pyspark import SparkConf
from pyspark import SparkContext

class FeaturePlatform(SparkSessionBase):
    """特征更新平台
    """
    SPARK_APP_NAME = "featureCenter"
    ENABLE_HIVE_SUPPORT = False
    def __init__(self):
        self.spark = self._create_spark_session()

    def loadData(self):
        df_moives = self.spark.read.csv("file:///root/Recom-Sys-Distributed/data/movies.csv", header="true")
        df_ratings = self.spark.read.csv("file:///root/Recom-Sys-Distributed/data/ratings.csv", header="true")

        def etl(partition):
            import re
            for row in partition:
                title = re.sub(r'\((\d*)\)', '', row.title)
                genres = row.genres.split("|")

                yield row.movieId, title, genres

        movies = df_moives.rdd.mapPartitions(etl).toDF(["movieId", "title", "genres"])
        df_ratings.join(movies,on='movieId')

if __name__ == '__main__':
    fp = FeaturePlatform()
