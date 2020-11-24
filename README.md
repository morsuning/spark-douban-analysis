# 使用方式

## 打包
```
mvn package
```

## 代码提交
```
cd target
```

以 Client 模式提交
```
spark-submit --master spark://namenode:7077 --deploy-mode client --executor-memory 512m --total-executor-cores 3 --class edu.nju.SparkStreamingApp spark-douban-analysis-1.0-SNAPSHOT.jar
```

以 Cluster 模式提交
```
spark-submit --master spark://namenode:7077 --deploy-mode cluster --executor-memory 512m --total-executor-cores 3 --class edu.nju.SparkStreamingApp  spark-douban-analysis-1.0-SNAPSHOT.jar
```