package edu.nju;

import edu.nju.config.ConfigurationManager;
import edu.nju.config.Constants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
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

            JavaDStream<String> data = stream.map(ConsumerRecord::key);

            /**
             * 下面给出一些无状态转换操作的含义：
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
             * transform(func)：通过对源DStream的每个RDD应用RDD-to-RDD函数，创建一个新的DStream。支持在新的DStream中做任何RDD操作。
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
