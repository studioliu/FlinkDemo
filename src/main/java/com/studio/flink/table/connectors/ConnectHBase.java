package com.studio.flink.table.connectors;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ConnectHBase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Tuple4<String, String, String, String>> eventStream = env
                .fromElements(
                        Tuple4.of("1001", "中国", "Alice", "16"),
                        Tuple4.of("1002", "中国", "Bob", "18"),
                        Tuple4.of("1003", "俄罗斯", "Mary", "17")
                );

        tableEnv.createTemporaryView("T", eventStream, Schema.newBuilder()
                .columnByExpression("rowkey", "f0")
                .columnByExpression("f1q1", "f1")
                .columnByExpression("f2q2", "f2")
                .columnByExpression("f2q3", "f3")
                .build());

        // 在 Flink SQL 中注册 HBase 表 "mytable"
        String createHbaseTable = "CREATE TABLE hTable (" +
                " rowkey STRING," +
                " family1 ROW<q1 STRING>," +
                " family2 ROW<q2 STRING, q3 STRING>," +
                " PRIMARY KEY (rowkey) NOT ENFORCED" +
                ") WITH (" +
                " 'connector' = 'hbase-2.2'," +
                " 'table-name' = 'mytable'," +
                " 'zookeeper.quorum' = 'master:2181'" +
                ")";
        tableEnv.executeSql(createHbaseTable);

        // 用 ROW(...) 构造函数构造列簇，并往 HBase 表写数据。
        // 假设 "T" 的表结构是 [rowkey, f1q1, f2q2, f2q3]
//        tableEnv.executeSql("INSERT INTO hTable " +
//                "SELECT rowkey, ROW(f1q1), ROW(f2q2, f2q3) FROM T");

        // 从 HBase 表扫描数据
        tableEnv.toDataStream(tableEnv.sqlQuery("select rowkey, family1.q1, family2 from hTable")).print();

        env.execute();
    }
}
