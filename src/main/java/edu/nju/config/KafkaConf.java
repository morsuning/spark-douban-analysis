package edu.nju.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Serializable;
import java.util.*;

/**
 * @author xuechenyang(morsuning @ gmail.com)
 * @date 2020/11/23 00:59
 */
public class KafkaConf implements Serializable {

    public static Map<String, Object> getKafkaParams() {
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
        return kafkaParams;
    }

    public static Set<String> getTopicsSet() {
        return new HashSet<>(
                Arrays.asList(ConfigurationManager.getProperty(Constants.KAFKA_TOPICS).split(",")));
    }
}
