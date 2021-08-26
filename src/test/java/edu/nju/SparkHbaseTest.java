package edu.nju;

import edu.nju.config.HbaseConf;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;

/**
 * @author xuechenyang(morsuning @ gmail.com)
 * @date 2020/11/24 05:31
 */
public class SparkHbaseTest implements Serializable {


    public static void main(String[] args) {
        try (Connection hbaseConn = HbaseConf.getHbaseConn()) {
            String rowKey = "recordOne";
            Table table = hbaseConn.getTable(TableName.valueOf("test"));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes("record"), Bytes.toBytes("json"), Bytes.toBytes("ValueTest"));
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
