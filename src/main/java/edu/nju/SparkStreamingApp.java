package edu.nju;

import edu.nju.config.ConfigurationManager;
import edu.nju.config.Constants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
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

            Set<String> topicsSet = new HashSet<>(
                    Arrays.asList(ConfigurationManager.getProperty(Constants.KAFKA_TOPICS).split(",")));

            JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

            JavaDStream<String> lines = stream.map(ConsumerRecord::value);

            lines.print();
            JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
            JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                    .reduceByKey(Integer::sum);

            wordCounts.print();

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
