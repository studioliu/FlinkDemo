package com.studio.flink.sink;

import com.studio.flink.source.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkToClickHouse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("input/clicks.csv");

        SingleOutputStreamOperator<Event> eventDS = source.map(e -> {
            String[] fields = e.split(",");
            return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
        });

        eventDS.addSink(JdbcSink.sink(
                "insert into clicks(user_name,url,ts) values(?,?,?)",
                (ps, e) -> {
                    ps.setString(1, e.user);
                    ps.setString(2, e.url);
                    ps.setLong(3, e.timestamp);
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUrl("jdbc:clickhouse://master:8123/default")
                        .build()
        ));

        env.execute();
    }
}
