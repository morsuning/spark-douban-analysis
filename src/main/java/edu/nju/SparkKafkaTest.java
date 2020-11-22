package edu.nju;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import edu.nju.config.ConfigurationManager;
import edu.nju.config.Constants;
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
                    ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

            JavaDStream<String> lines = stream.map(ConsumerRecord::value);

            lines.print();

            JavaDStream<String> data1 = stream.map(new Function<ConsumerRecord<String, String>, String>() {
                @Override
                public String call(ConsumerRecord<String, String> stringConsumerRecord) throws Exception {
                    String jsonData = stringConsumerRecord.value();
                    String value = "";
                    JSONObject jsonObject = new JSONObject().getJSONObject(jsonData);
                    for (String key : jsonObject.keySet()) {
                        if ("title".equals(key)) {
                            value = jsonObject.getString(key);
                        }
                    }
                    return value;
                }
            });
            data1.print();

//            JavaDStream<String> data2 = stream.map(new Function<ConsumerRecord<String, String>, String>() {
//                @Override
//                public String call(ConsumerRecord<String, String> stringConsumerRecord) throws Exception {
//                    String jsonData = stringConsumerRecord.value();
//                    String value = "";
//                    JSONObject jsonObject = JSON.parseObject(jsonData);
//                    for (String key : jsonObject.keySet()) {
//                        if ("seen".equals(key)) {
//                            value = jsonObject.getString(key);
//                        }
//                    }
//                    return value;
//                }
//            });
//
//            data1.print();
//            data2.print();

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
        SparkKafkaTest sparkKafkaTest = new SparkKafkaTest();
        sparkKafkaTest.start();
    }
}
