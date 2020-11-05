# 增加spark online 启动配置
class DefaultConfig(object):
    """默认的一些配置信息
    """
    # 在线计算spark配置
    SPARK_ONLINE_CONFIG = (
        ("spark.app.name", "onlineUpdate"),  # 设置启动的spark的app名称，没有提供，将随机产生一个名称
        ("spark.master", "yarn"),
        ("spark.executor.instances", 4)
    )

    # KAFKA配置
    KAFKA_SERVER = "192.168.31.99:9092"


