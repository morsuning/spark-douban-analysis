package edu.nju;

import com.alibaba.fastjson.JSON;
import edu.nju.config.ConfigurationManager;
import edu.nju.config.Constants;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

/**
 * @author xuechenyang(morsuning @ gmail.com)
 * @date 2020/11/19 01:01
 */
public class SparkStreamingApp {

    private static final Pattern SPACE = Pattern.compile(" ");

    SparkConf sparkConf = new SparkConf().setAppName(Constants.APP_NAME);
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    public void start() {
        sc.setLogLevel("WARN");

        try (JavaStreamingContext jssc = new JavaStreamingContext(this.sc, Durations.seconds(10))) {

            jssc.checkpoint("hdfs:///spark/streaming_checkpoint");


            Map<String, Object> kafkaParams = new HashMap<>(4);
            kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigurationManager.getProperty(Constants.KAFKA_BOOTSTRAP_SERVERS));
            kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, ConfigurationManager.getProperty(Constants.GROUP_ID));
            kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

            //指定从latest(最新,其他版本的是largest这里不行)还是smallest(最早)处开始读取数据
//            kafkaParams.put("auto.offset.reset", "latest");
            //如果true,consumer定期地往zookeeper写入每个分区的offset
//            kafkaParams.put("enable.auto.commit", false);

//            Map<TopicPartition, Long> offset = new HashMap<>();
//            offset.put(new TopicPartition("", 0), 0L);

            Set<String> topicsSet = new HashSet<>(
                    Arrays.asList(ConfigurationManager.getProperty(Constants.KAFKA_TOPICS).split(",")));

            JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(topicsSet, kafkaParams /**, offset**/ ));


            JavaDStream<String> data = stream.map(new Function<ConsumerRecord<String, String>, String>() {
                @Override
                public String call(ConsumerRecord<String, String> stringConsumerRecord) throws Exception {
                    String jsonData = stringConsumerRecord.value();
                    JSONObject jsonObject = JSON.parseObject(jsonData);
                    for (String key : jsonObject.keySet()) {
                        String value = jsonObject.getString(key);

                    }
                    return "stringConsumerRecord";
                }
            });

            /**
             * 一些无状态转换操作的含义：
             * map(func) ：对源DStream的每个元素，采用func函数进行转换，得到一个新的DStream；
             * flatMap(func)： 与map相似，但是每个输入项可用被映射为0个或者多个输出项；
             * filter(func)： 返回一个新的DStream，仅包含源DStream中满足函数func的项；
             * repartition(numPartitions)： 通过创建更多或者更少的分区改变DStream的并行程度；
             * union(otherStream)： 返回一个新的DStream，包含源DStream和其他DStream的元素；
             * count()：统计源DStream中每个RDD的元素数量；
             * reduce(func)：利用函数func聚集源DStream中每个RDD的元素，返回一个包含单元素RDDs的新DStream；
             * countByValue()：应用于元素类型为K的DStream上，返回一个（K，V）键值对类型的新DStream，每个键的值是在原DStream的每个RDD中的出现次数；
             * reduceByKey(func, [numTasks])：当在一个由(K,V)键值对组成的DStream上执行该操作时，返回一个新的由(K,V)键值对组成的DStream，每一个key的值均由给定的recuce函数（func）聚集起来；
             * join(otherStream, [numTasks])：当应用于两个DStream（一个包含（K,V）键值对,一个包含(K,W)键值对），返回一个包含(K, (V, W))键值对的新DStream；
             * cogroup(otherStream, [numTasks])：当应用于两个DStream（一个包含（K,V）键值对,一个包含(K,W)键值对），返回一个包含(K, Seq[V], Seq[W])的元组；
             * transform(func)：将 DStream 转化成 RDD，通过对源DStream的每个RDD应用RDD-to-RDD函数，创建一个新的DStream。支持在新的DStream中做任何RDD操作。
             */

            /**
             * 一些窗口转换操作
             * window(windowLength,slideInterval): 返回一个基于源 DStream 的窗口批次计算得到新的 DStream
             * countByWindow(windowLength,slideInterval): 返回基于滑动窗口的 DStream 中的元素的数量
             * reduceByWindow(func,windowLength,slideInterval): 基于滑动窗口对源 DStream 中的元素进行聚合操作，得到一个新的 DStream
             * reduceByKeyAndWindow(func,windowLength,slideInterval,[numTasks]): 基于滑动窗口对 <K,V> 键值对类型的 DStream 中的值按 K 使用聚合函数 func 进行聚合操作，得到一个新的 DStream
             * reduceByKeyAndWindow(func,invFunc,windowLength,slideInterval,[numTasks]): 一个更高效的实现版本，先对滑动窗口中新的时间间隔内的数据进行增量聚合，再移去最早的同等时间间隔内的数据统计量。例如，计算 t+4 秒这个时刻过去 5 秒窗口的 WordCount 时，可以将 t+3 时刻过去 5 秒的统计量加上 [t+3,t+4] 的统计量，再减去 [t-2,t-1] 的统计量，这种方法可以复用中间 3 秒的统计量，提高统计的效率
             * countByValueAndWindow(windowLength,slideInterval,[numTasks]): 基于滑动窗口计算源 DStream 中每个 RDD 内每个元素出现的频次，并返回 DStream[<K,Long>]，其中，K 是 RDD 中元素的类型，Long 是元素频次。Reduce 任务的数量可以通过一个可选参数进行配置
             */

            /**
             * 输出操作
             * print(): 在 Driver 中打印出 DStream 中数据的前 10 个元素
             * saveAsTextFiles(prefix,[suffix]): 将 DStream 中的内容以文本的形式保存为文本文件，其中，每次批处理间隔内产生的文件以 prefix-TIME_IN_MS[.suffix] 的方式命名
             * saveAsObjectFiles(prefix,[suffix]): 将 DStream 中的内容按对象序列化，并且以 SequenceFile 的格式保存，其中，每次批处理间隔内产生的文件以 prefix—TIME_IN_MS[.suffix]的方式命名
             * saveAsHadoopFiles(prefix,[suffix]): 将 DStream 中的内容以文本的形式保存为 Hadoop 文件，其中，每次批处理间隔内产生的文件以 prefix-TIME_IN_MS[.suffix] 的方式命名
             * foreachRDD(func): 最基本的输出操作，将 func 函数应用于 DStream 中的 RDD 上，这个操作会输出数据到外部系统，例如，保存 RDD 到文件或者网络数据库等。需要注意的是，func 函数是在该 Streaming 应用的 Driver 进程里执行的
             */

            JavaDStream<String> lines = stream.map(ConsumerRecord::value).cache();

            lines.print();

            // 拿到每个元素
            JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());


            // 分离每组
            JavaPairDStream<String, Integer> dataOne = lines.mapToPair(s -> new Tuple2<>(s, 1));



//            JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
//                    .reduceByKey(Integer::sum);

            jssc.start();
            jssc.awaitTermination();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void compute(JavaDStream<String> lines) {

    }

    private void writeToHbase() {

    }


    public static void main(String[] args) {
        SparkStreamingApp sparkStreamingApp = new SparkStreamingApp();
        sparkStreamingApp.start();
    }
}
