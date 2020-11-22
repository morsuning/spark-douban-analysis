package edu.nju.config;

/**
 * @author xuechenyang(morsuning @ gmail.com)
 * @date 2020/11/21 22:30
 */
public interface Constants {
    String APP_NAME = "spark-douban-analysis";

    String MASTER = "spark://namenode:7077";

    String KAFKA_BOOTSTRAP_SERVERS = "bootstrap.servers";

    String GROUP_ID = "group.id";

    String KAFKA_TOPICS = "kafka.topics";
}
