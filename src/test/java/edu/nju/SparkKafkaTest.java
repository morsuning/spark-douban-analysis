package edu.nju;

import edu.nju.config.Constants;
import edu.nju.config.KafkaConf;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
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
 * @date 2020/11/21 14:03
 */
public class SparkKafkaTest implements Serializable {

    SparkConf sparkConf = new SparkConf().setAppName(Constants.APP_NAME).setMaster(Constants.MASTER);
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    public void test() {

    }

    public void start() {
        sc.setLogLevel("WARN");

        try (JavaStreamingContext jssc = new JavaStreamingContext(this.sc, Durations.seconds(10))) {

            JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(KafkaConf.getTopicsSet(), KafkaConf.getKafkaParams()));

            JavaDStream<String> lines = stream.map(ConsumerRecord::value);

            lines.print();

            System.out.println(lines.toString());

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
