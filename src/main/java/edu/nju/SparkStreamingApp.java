package edu.nju;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import edu.nju.config.Constants;
import edu.nju.config.KafkaConf;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

/**
 * @author xuechenyang(morsuning @ gmail.com)
 * @date 2020/11/19 01:01
 */
public class SparkStreamingApp implements Serializable {

    SparkConf sparkConf = new SparkConf().setAppName(Constants.APP_NAME);
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    public void start() {
        sc.setLogLevel("WARN");

        try (JavaStreamingContext jssc = new JavaStreamingContext(this.sc, Durations.seconds(10))) {

            jssc.checkpoint("hdfs:///spark/streaming_checkpoint");

            JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    // 可以有第三个参数 offset
                    ConsumerStrategies.Subscribe(KafkaConf.getTopicsSet(), KafkaConf.getKafkaParams()));

            // 剧名
            JavaDStream<String> title = stream
                    .map(new Function<ConsumerRecord<String, String>, String>() {
                             @Override
                             public String call(ConsumerRecord<String, String> consumerRecord){
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

            title.print();

//            // 想看，在看，看过总数
//            JavaDStream<Integer> count_awaiting_warching_seen = stream
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
            e.printStackTrace();
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
