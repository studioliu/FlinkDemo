package com.studio.flink.table.connectors;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ConnectKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.createTemporaryTable("KafkaSourceTable", TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .column("user_name", DataTypes.STRING())
                        .column("url", DataTypes.STRING())
                        .column("ts", DataTypes.BIGINT())
                        .build())
                .option("topic", "clicks")
                .option("properties.bootstrap.servers", "master:9092")
                .option("properties.group.id", "testGroup")
                .option("scan.startup.mode", "latest-offset") //`earliest-offset`：从可能的最早偏移量开始。`latest-offset`：从最末尾偏移量开始。
                .format("csv")
                .build());

        tableEnv.createTemporaryTable("KafkaSinkTable", TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .column("user_name", DataTypes.STRING())
                        .column("url", DataTypes.STRING())
                        .column("ts", DataTypes.BIGINT())
                        .build())
                .option("topic", "events")
                .option("properties.bootstrap.servers", "master:9092")
                .format("json")
                .build());

        tableEnv.executeSql("insert into KafkaSinkTable select * from KafkaSourceTable");

    }
}
