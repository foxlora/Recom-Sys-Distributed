## 大数据版本电影推荐系统

### 一、系统架构

![image-20201106200925035](https://github.com/foxlora/Recom-Sys-Distributed/blob/master/README.assets/image-20201106200925035.png)



离线部分：

- 用户行为日志采集 ==> 获取用户的行为特征
- 基于离线数据，Spark处理获得用户画像，item画像，保存在Hbase
- 将处理后的特征输入到TensforFlow进行模型训练
- 召回，排序

在线部分：

- Flume采集日志数据 ==> kafka ==> Spark Streaming进行处理
- 可以基于共现关系得到与新用户的相似用户
