package edu.nju;

import edu.nju.config.SparkConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author xuechenyang(morsuning @ gmail.com)
 * @date 2020/11/19 01:01
 */
public class SparkStreamingApp {


    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(SparkConfig.APP_NAME).setMaster(SparkConfig.MASTER);

        try (JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));){

            JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

            lines.map(new Function<String, Object>() {
                @Override
                public Object call(String s) throws Exception {
                    return null;
                }
            });

            JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());


            JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
            JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(Integer::sum);

            wordCounts.print();

            jssc.start();
            jssc.awaitTermination();



        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
