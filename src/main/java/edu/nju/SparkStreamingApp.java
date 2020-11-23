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
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.Serializable;
import java.util.*;

/**
 * @author xuechenyang(morsuning @ gmail.com)
 * @date 2020/11/19 01:01
 */
public class SparkStreamingApp implements Serializable {

    private final JavaSparkContext sc;

    SparkStreamingApp() {
        SparkConf sparkConf = new SparkConf().setAppName(Constants.APP_NAME);
        this.sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");
    }

    public void start() {
        try (JavaStreamingContext jssc = new JavaStreamingContext(this.sc, Durations.seconds(10))) {

            jssc.checkpoint("hdfs:///spark/streaming_checkpoint");

            Set<String> topicSet = new HashSet<>(
                    Arrays.asList(ConfigurationManager.getProperty(Constants.KAFKA_TOPICS).split(",")));

            Map<String, Object> kafkaParams = new HashMap<>(4);
            kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigurationManager.getProperty(Constants.KAFKA_BOOTSTRAP_SERVERS));
            kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, ConfigurationManager.getProperty(Constants.GROUP_ID));
            kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

            JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    // 可以有第三个参数 offset
                    ConsumerStrategies.Subscribe(topicSet, kafkaParams));

            // 剧名
            JavaDStream<String> title = stream.map(new Function<ConsumerRecord<String, String>, String>() {
                @Override
                public String call(ConsumerRecord<String, String> consumerRecord){
                    return consumerRecord.value();
                }
            });

            title.print();

//            // 想看，在看，看过总数
//            JavaDStream<Integer> count_awaiting_watching_seen = stream
//                    .map(stringConsumerRecord -> getVal(stringConsumerRecord, Constants.AWAITING))
//                    .map(Integer::parseInt)
//                    .union(stream
//                            .map(stringConsumerRecord -> getVal(stringConsumerRecord, Constants.WATCHING))
//                            .map(Integer::parseInt))
//                    .union(stream
//                            .map(stringConsumerRecord -> getVal(stringConsumerRecord, Constants.SEEN))
//                            .map(Integer::parseInt)
//                            .reduce(Integer::sum));
//
//            // 短评 + 剧评总数
//            JavaDStream<Integer> count_comment = stream
//                    .map(stringConsumerRecord -> getVal(stringConsumerRecord, Constants.SHORT_COMMENT_COUNT))
//                    .map(Integer::parseInt)
//                    .union(stream
//                            .map(stringConsumerRecord -> getVal(stringConsumerRecord, Constants.COMMENT_COUNT))
//                            .map(Integer::parseInt))
//                    .reduce(Integer::sum);

//            // 计算加权原始热度
//            JavaDStream<Integer> awaiting = stream.map(
//                    stringConsumerRecord -> getVal(stringConsumerRecord, Constants.AWAITING))
//                    .map(Integer::parseInt);
//
//            JavaDStream<Integer> watching = stream.map(
//                    stringConsumerRecord -> getVal(stringConsumerRecord, Constants.WATCHING))
//                    .map(s -> Integer.parseInt(s) * 2);
//
//            JavaDStream<Integer> seen = stream.map(
//                    stringConsumerRecord -> getVal(stringConsumerRecord, Constants.SEEN))
//                    .map(s -> Integer.parseInt(s) * 3);
//
//            JavaDStream<Integer> short_comment_count = stream.map(
//                    stringConsumerRecord -> getVal(stringConsumerRecord, Constants.SHORT_COMMENT_COUNT))
//                    .map(s -> Integer.parseInt(s) * 4);
//
//            JavaDStream<Integer> comment_count = stream.map(
//                    stringConsumerRecord -> getVal(stringConsumerRecord, Constants.COMMENT_COUNT))
//                    .map(s -> Integer.parseInt(s) * 4);
//
//            JavaDStream<Integer> comment_reply_count = stream.map(
//                    stringConsumerRecord -> getVal(stringConsumerRecord, Constants.COMMENT_REPLY_COUNT))
//                    .map(s -> Integer.parseInt(s) * 3);
//
//            JavaDStream<Integer> comment_usefulness_count = stream.map(
//                    stringConsumerRecord -> getVal(stringConsumerRecord, Constants.COMMENT_USEFULNESS_COUNT))
//                    .map(Integer::parseInt);
//
//            JavaDStream<Integer> comment_share_count = stream.map(
//                    stringConsumerRecord -> getVal(stringConsumerRecord, Constants.COMMENT_SHARE_COUNT))
//                    .map(s -> Integer.parseInt(s) * 2);
//
//            JavaDStream<Integer> comment_collect_count = stream.map(
//                    stringConsumerRecord -> getVal(stringConsumerRecord, Constants.COMMENT_COLLECT_COUNT))
//                    .map(s -> Integer.parseInt(s) * 3);

            jssc.start();
            jssc.awaitTermination();


//            JavaDStream<Integer> short_comment_like_count = stream.map(
//                    stringConsumerRecord -> getVal(stringConsumerRecord, Constants.SHORT_COMMENT_LIKE_COUNT))
//                    .map(Integer::parseInt);

        } catch (Exception e) {
        }
    }

    private String getVal(ConsumerRecord<String, String> consumerRecord, String s) {
        String jsonData = consumerRecord.value();
        String value = "";
        JSONObject jsonObject = JSON.parseObject(jsonData);
        for (String key : jsonObject.keySet()) {
            if (s.equals(key)) {
                value = jsonObject.getString(key);
            }
        }
        return value;
    }

    private void writeToHbase() {

    }

    public static void main(String[] args) {
        SparkStreamingApp sparkStreamingApp = new SparkStreamingApp();
        sparkStreamingApp.start();
    }
}
