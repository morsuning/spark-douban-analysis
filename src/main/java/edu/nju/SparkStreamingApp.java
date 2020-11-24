package edu.nju;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import edu.nju.config.ConfigManager;
import edu.nju.config.Constants;
import edu.nju.config.HbaseConf;
import edu.nju.config.KafkaConf;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author xuechenyang(morsuning @ gmail.com)
 * @date 2020/11/19 01:01
 */
public class SparkStreamingApp implements Serializable {

    public static void main(String[] args) {
        SparkStreamingApp sparkStreamingApp = new SparkStreamingApp();
        sparkStreamingApp.start();
    }

    public void start() {
        SparkConf sparkConf = new SparkConf().setAppName(Constants.APP_NAME);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");
        try (JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.minutes(60))) {

            jssc.checkpoint("hdfs:///spark/streaming_checkpoint");

            JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(KafkaConf.getTopicsSet(), KafkaConf.getKafkaParams()));

            // 短评 + 剧评总数
            JavaDStream<Integer> totalComment = stream
                    .map(consumerRecord -> getVal(consumerRecord, Constants.SHORT_COMMENT_COUNT))
                    .map(Integer::parseInt)
                    .union(stream
                            .map(consumerRecord -> getVal(consumerRecord, Constants.COMMENT_COUNT))
                            .map(Integer::parseInt))
                    .reduce(Integer::sum).cache();

            // 计算加权原始热度
            JavaDStream<Integer> originalHeat = stream.map(consumerRecord -> {
                String currentID = getVal(consumerRecord, Constants.ID);
                int awaiting = Integer.parseInt(getVal(consumerRecord, Constants.AWAITING));
                int watching = 2 * Integer.parseInt(getVal(consumerRecord, Constants.WATCHING));
                int seen = 3 * Integer.parseInt(getVal(consumerRecord, Constants.SEEN));
                int countShortComment = 4 * Integer.parseInt(getVal(consumerRecord, Constants.SHORT_COMMENT_COUNT));
                int countComment = 4 * Integer.parseInt(getVal(consumerRecord, Constants.COMMENT_COUNT));
                int countReplyComment = 3 * Integer.parseInt(getVal(consumerRecord, Constants.COMMENT_REPLY_COUNT));
                int countUsefulnessComment = Integer.parseInt(getVal(consumerRecord, Constants.COMMENT_USEFULNESS_COUNT));
                int countShareComment = 2 * Integer.parseInt(getVal(consumerRecord, Constants.COMMENT_SHARE_COUNT));
                int countCollectComment = 3 * Integer.parseInt(getVal(consumerRecord, Constants.COMMENT_COLLECT_COUNT));
                int res = awaiting + watching + seen + countShortComment + countComment
                        + countReplyComment + countUsefulnessComment + countShareComment
                        + countCollectComment;
                if (idHeatMap.containsKey(currentID)) {
                    idDeltaHeatMap.put(currentID, res - idHeatMap.get(currentID));
                }
                idHeatMap.put(currentID, res);
                return res;
            });

            // 计算衰减指数
            JavaDStream<Integer> decayHeat = stream
                    .map(consumerRecord -> {
                        String currentId = getVal(consumerRecord, Constants.ID);
                        String currentTime = getVal(consumerRecord, Constants.TIME);
                        if (!idTimeMap.containsKey(currentId)) {
                            idTimeMap.put(currentId, Integer.parseInt(currentTime));
                            return 1;
                        } else {
                            // 衰减因子
                            int r = 8;
                            int tLast = idTimeMap.get(currentId);
                            double index = (-r * (Integer.parseInt(currentTime) - tLast)) / (864000 * 1.0);
                            return (int) Math.pow(Math.E, index);
                        }
                    });

            // 热度
            JavaDStream<Integer> heat = originalHeat
                    .union(decayHeat)
                    .reduce((integer, integer2) -> integer * integer2);

            // 变化量热度
            JavaDStream<Integer> deltaHeat = stream.map(consumerRecord -> {
                String deltaHeatId = getVal(consumerRecord, Constants.ID);
                int res = 0;
                if (idDeltaHeatMap.containsKey(deltaHeatId)) {
                    res = idDeltaHeatMap.get(deltaHeatId);
                }
                return res;
            });

            // 总热度
            JavaDStream<Integer> totalHeat = heat.union(deltaHeat).reduce(Integer::sum).cache();

            // 保存数据
            stream.foreachRDD((consumerRecordJavaRdd, time) ->
                    consumerRecordJavaRdd.foreach(consumerRecord -> {
                        String id = getVal(consumerRecord, Constants.ID);
                        String title = getVal(consumerRecord, Constants.TITLE);
                        String score = getVal(consumerRecord, Constants.SCORE);
                        String awaiting = getVal(consumerRecord, Constants.AWAITING);
                        String watching = getVal(consumerRecord, Constants.WATCHING);
                        String seen = getVal(consumerRecord, Constants.SEEN);
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put(Constants.TITLE, title);
                        jsonObject.put(Constants.SCORE, score);
                        jsonObject.put(Constants.AWAITING, awaiting);
                        jsonObject.put(Constants.WATCHING, watching);
                        jsonObject.put(Constants.SEEN, seen);
                        // TODO 测试 结果也许不一致
                        jsonObject.put(Constants.COMMENT_COUNT, totalComment.toString());
                        jsonObject.put(Constants.TOTAL_HEAT, totalHeat.toString());
                        Connection hbaseConn = HbaseConf.getHbaseConn();
                        Table table = hbaseConn.getTable(TableName.valueOf(ConfigManager.getProperty(Constants.TABLE_NAME)));
                        Put put = new Put(Bytes.toBytes(id));
                        put.addColumn(Bytes.toBytes("record"), Bytes.toBytes("json"), Bytes.toBytes(jsonObject.toJSONString()));
                        table.put(put);
                        table.close();
                    }));

            JavaDStream<String> message = stream.map(consumerRecord -> new Date().toString() + "A record has been added to the table \"douban_anime_statis_simple\"");
            message.print();

            jssc.start();
            jssc.awaitTermination();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 记录计算初始热度的时间
     */
    private Map<String, Integer> idTimeMap = new HashMap<>();

    /**
     * 记录上一次计算出来的原始热度
     */
    private Map<String, Integer> idHeatMap = new HashMap<>();

    /**
     * 记录本次的当时变化量热度
     */
    private Map<String, Integer> idDeltaHeatMap = new HashMap<>();

    /**
     * 根据 Key 提取 JSON String 的 Value
     *
     * @param consumerRecord Kafka 数据对象
     * @param key            Key
     * @return Value
     */
    private String getVal(ConsumerRecord<String, String> consumerRecord, String key) {
        return JSON.parseObject(consumerRecord.value()).getString(key);
    }
}
