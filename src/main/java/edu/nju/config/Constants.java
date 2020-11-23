package edu.nju.config;

import java.io.Serializable;

/**
 * @author xuechenyang(morsuning @ gmail.com)
 * @date 2020/11/21 22:30
 */
public interface Constants extends Serializable {
    String APP_NAME = "spark-douban-analysis";

    String MASTER = "spark://namenode:7077";

    String KAFKA_BOOTSTRAP_SERVERS = "bootstrap.servers";

    String GROUP_ID = "group.id";

    String KAFKA_TOPICS = "kafka.topics";


    String TIME = "time";

    String TITLE = "title";

    String SCORE = "score";

    String AWAITING = "awaiting";

    String WATCHING = "watching";

    String SEEN = "seen";

    String SHORT_COMMENT_COUNT = "short_comment_count";

    String SHORT_COMMENT_LIKE_COUNT = "short_comment_like_count";

    String COMMENT_COUNT = "comment_count";

    String COMMENT_REPLY_COUNT = "comment_reply_count";

    String COMMENT_USEFULNESS_COUNT = "comment_usefulness_count";

    String COMMENT_SHARE_COUNT = "comment_share_count";

    String COMMENT_COLLECT_COUNT ="comment_collect_count";
}
