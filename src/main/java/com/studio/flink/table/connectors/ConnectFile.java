package com.studio.flink.table.connectors;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ConnectFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1).setRuntimeMode(RuntimeExecutionMode.BATCH);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.createTemporaryTable("FileSourceTable", TableDescriptor.forConnector("filesystem")
                .schema(Schema.newBuilder()
                        .column("user_name", DataTypes.STRING())
                        .column("url", DataTypes.STRING())
                        .column("ts", DataTypes.BIGINT())
                        .build())
                .format("csv")
                .option("path", "input/clicks.csv")
                .build());

        tableEnv.createTemporaryTable("FileSinkTable", TableDescriptor.forConnector("filesystem")
                .schema(Schema.newBuilder()
                        .column("user_name", DataTypes.STRING())
                        .column("cnt", DataTypes.BIGINT())
                        .build())
                .format("json")
                .option("path", "output")
                .build());

        tableEnv.executeSql("INSERT INTO FileSinkTable " +
                "SELECT user_name,count(url) FROM FileSourceTable " +
                "GROUP BY user_name");

    }
}
