package edu.nju.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author xuechenyang(morsuning @ gmail.com)
 * @date 2020/11/23 01:08
 */
public class HbaseConf implements Serializable {

    public static Connection getHbaseConn() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",
                ConfigManager.getProperty(Constants.HBASE_ZOOKEEPER_QUORUM));
        conf.set("hbase.zookeeper.property.clientPort",
                ConfigManager.getProperty(Constants.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT));
//        conf.set("hbase.master", "namenode:60000");
        return ConnectionFactory.createConnection(conf);
    }
}
