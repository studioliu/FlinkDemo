package com.studio.flink.table.connectors;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;
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
                .option(KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS.key(), "master:9092")
                .option(KafkaConnectorOptions.TOPIC.key(), "clicks")
                .option(KafkaConnectorOptions.PROPS_GROUP_ID.key(), "Consumer_Group01")
                .option(KafkaConnectorOptions.SCAN_STARTUP_MODE.key(),
                        KafkaConnectorOptions.ScanStartupMode.LATEST_OFFSET.name())
                .format("csv")
                .build());

        tableEnv.createTemporaryTable("KafkaSinkTable", TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .column("user_name", DataTypes.STRING())
                        .column("url", DataTypes.STRING())
                        .column("ts", DataTypes.BIGINT())
                        .build())
                .option(KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS.key(), "master:9092")
                .option(KafkaConnectorOptions.TOPIC.key(), "events")
                .format("json")
                .build());

        tableEnv.executeSql("insert into KafkaSinkTable select * from KafkaSourceTable");

    }
}
