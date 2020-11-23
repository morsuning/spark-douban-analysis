package edu.nju;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import edu.nju.config.ConfigurationManager;
import edu.nju.config.Constants;
import edu.nju.config.KafkaConf;
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
 * @date 2020/11/21 14:03
 */
public class SparkKafkaTest {

    private static final Pattern SPACE = Pattern.compile(" ");

    SparkConf sparkConf = new SparkConf().setAppName(Constants.APP_NAME);
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    public void start() {
        sc.setLogLevel("WARN");

        try (JavaStreamingContext jssc = new JavaStreamingContext(this.sc, Durations.seconds(10))) {

            JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(KafkaConf.getTopicsSet(), KafkaConf.getKafkaParams()));

            JavaDStream<String> lines = stream.map(
                    new Function<ConsumerRecord<String, String>, String>() {
                        @Override
                        public String call(ConsumerRecord<String, String> consumerRecord) {
                            String jsonData = consumerRecord.value();
                            String value = "";
                            JSONObject jsonObject = JSON.parseObject(jsonData);
                            for (String key : jsonObject.keySet()) {
                                if ("TITLE".equals(key)) {
                                    value = jsonObject.getString(key);
                                }
                            }
                            return value;
                        }
                    }
            );

            lines.print();

            // 拿到每个元素
            JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

            JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                    .reduceByKey(Integer::sum);
            wordCounts.print();

            jssc.start();
            jssc.awaitTermination();

        } catch (Exception e) {
        }
    }

    public static void main(String[] args) {
        SparkKafkaTest sparkKafkaTest = new SparkKafkaTest();
        sparkKafkaTest.start();
    }
}
