package edu.nju;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import edu.nju.config.Constants;
import edu.nju.config.KafkaConf;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
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


    public void start() {
        SparkConf sparkConf = new SparkConf().setAppName(Constants.APP_NAME);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");
        try (JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(10))) {

            jssc.checkpoint("hdfs:///spark/streaming_checkpoint");

            JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    // 可以有第三个参数 offset
                    ConsumerStrategies.Subscribe(KafkaConf.getTopicsSet(), KafkaConf.getKafkaParams()));

            // 剧名
            JavaDStream<String> title = stream.map(
                    consumerRecord -> getVal(consumerRecord, Constants.TITLE)
            );

            // 评分
            JavaDStream<String> score = stream.map(
                    consumerRecord -> getVal(consumerRecord, Constants.SCORE)
            );

            // 想看，在看，看过总数
            JavaDStream<Integer> countAwaitingWatchingSeen = stream
                    .map(stringConsumerRecord -> getVal(stringConsumerRecord, Constants.AWAITING))
                    .map(Integer::parseInt)
                    .union(stream
                            .map(stringConsumerRecord -> getVal(stringConsumerRecord, Constants.WATCHING))
                            .map(Integer::parseInt))
                    .union(stream
                            .map(stringConsumerRecord -> getVal(stringConsumerRecord, Constants.SEEN))
                            .map(Integer::parseInt))
                    .reduce(Integer::sum);


            // 短评 + 剧评总数
            JavaDStream<Integer> count_comment = stream
                    .map(stringConsumerRecord -> getVal(stringConsumerRecord, Constants.SHORT_COMMENT_COUNT))
                    .map(Integer::parseInt)
                    .union(stream
                            .map(stringConsumerRecord -> getVal(stringConsumerRecord, Constants.COMMENT_COUNT))
                            .map(Integer::parseInt))
                    .reduce(Integer::sum);


            // 计算加权原始热度
            JavaDStream<Integer> awaiting = stream.map(
                    stringConsumerRecord -> getVal(stringConsumerRecord, Constants.AWAITING))
                    .map(Integer::parseInt);

            JavaDStream<Integer> watching = stream.map(
                    stringConsumerRecord -> getVal(stringConsumerRecord, Constants.WATCHING))
                    .map(s -> Integer.parseInt(s) * 2);

            JavaDStream<Integer> seen = stream.map(
                    stringConsumerRecord -> getVal(stringConsumerRecord, Constants.SEEN))
                    .map(s -> Integer.parseInt(s) * 3);

            JavaDStream<Integer> short_comment_count = stream.map(
                    stringConsumerRecord -> getVal(stringConsumerRecord, Constants.SHORT_COMMENT_COUNT))
                    .map(s -> Integer.parseInt(s) * 4);

            JavaDStream<Integer> comment_count = stream.map(
                    stringConsumerRecord -> getVal(stringConsumerRecord, Constants.COMMENT_COUNT))
                    .map(s -> Integer.parseInt(s) * 4);

            JavaDStream<Integer> comment_reply_count = stream.map(
                    stringConsumerRecord -> getVal(stringConsumerRecord, Constants.COMMENT_REPLY_COUNT))
                    .map(s -> Integer.parseInt(s) * 3);

            JavaDStream<Integer> comment_usefulness_count = stream.map(
                    stringConsumerRecord -> getVal(stringConsumerRecord, Constants.COMMENT_USEFULNESS_COUNT))
                    .map(Integer::parseInt);

            JavaDStream<Integer> comment_share_count = stream.map(
                    stringConsumerRecord -> getVal(stringConsumerRecord, Constants.COMMENT_SHARE_COUNT))
                    .map(s -> Integer.parseInt(s) * 2);

            JavaDStream<Integer> comment_collect_count = stream.map(
                    stringConsumerRecord -> getVal(stringConsumerRecord, Constants.COMMENT_COLLECT_COUNT))
                    .map(s -> Integer.parseInt(s) * 3);

            // 计算原始热度
            JavaDStream<Integer> originalHeat = awaiting
                    .union(watching)
                    .union(seen)
                    .union(short_comment_count)
                    .union(comment_count)
                    .union(comment_reply_count)
                    .union(comment_usefulness_count)
                    .union(comment_share_count)
                    .union(comment_collect_count)
                    .reduce(Integer::sum);

            originalHeat.print();

            // 计算衰减热度
            JavaDStream<Integer> decayHeat = stream
                    .map(consumerRecord -> getVal(consumerRecord, Constants.TIME))
                    .map(
                            (Function<String, Integer>) s -> {
                                int time = Integer.parseInt(s);
                                int r = 8;
                                // TODO 当前时间减去第一次收集信息的时间
                                int t = 10, tLast = 20;
                                double index = (- r * ( t - tLast)) / (864000 * 1.0);
                                return (int)Math.pow(Math.E, index);
                            }
                    );

            // 热度
            JavaDStream<Integer> heat = originalHeat
                    .union(decayHeat)
                    .reduce((integer, integer2) -> integer * integer2);

            // TODO 当时变化量热度
            JavaDStream<Integer> deltaHeat = null;

            // 总热度
            JavaDStream<Integer> totalHeat = heat.union(deltaHeat).reduce(Integer::sum);

            // TODO 存入 HBase 一：展示所需数据

            // TODO 存入 HBase 二：上次数据

            // TODO 查最新数据，利用这次数据计算

            jssc.start();
            jssc.awaitTermination();

        } catch (Exception e) {
        }
    }

    /**
     * 解析 JSON 数据
     * @param consumerRecord Kafka 数据对象
     * @param s 提取的条目
     * @return 提取的值
     */
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
