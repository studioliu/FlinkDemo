package com.studio.flink.table.connectors;

import com.studio.flink.source.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ConnectMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L));
        Table sourceTable = tableEnv.fromDataStream(stream);

        tableEnv.createTemporaryTable("SinkTable", TableDescriptor.forConnector("jdbc")
                .schema(Schema.newBuilder()
                        .column("user", DataTypes.STRING())
                        .column("url", DataTypes.STRING())
                        .column("ts", DataTypes.BIGINT())
                        .build())
                .option("url", "jdbc:mysql://localhost:3306/test")
                .option("table-name", "clicks")
                .option("username", "root")
                .option("password", "password")
                .build());

        sourceTable.executeInsert("SinkTable");
        tableEnv.toDataStream(tableEnv.sqlQuery("select * from SinkTable")).print();

        env.execute();
    }
}
