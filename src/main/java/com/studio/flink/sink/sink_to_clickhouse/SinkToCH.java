package com.studio.flink.sink.sink_to_clickhouse;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/* clickhouse建表语句
create table clicks(　
user_name String,　
url String,　
ts Int32　
) engine = MergeTree　
partition by user_name　
order by ts　;
 */

public class SinkToCH {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStream = env.readTextFile("input/clicks.csv");
        SingleOutputStreamOperator<String[]> mapDS = dataStream.map(data -> data.split(","));

        String sql = "insert into clicks(user_name,url,ts) values(?,?,?)";
        mapDS.addSink(new MyClickHouseSink(sql));

        env.execute();
    }

    public static class MyClickHouseSink extends RichSinkFunction<String[]> {
        Connection conn = null;
        PreparedStatement psmt = null;
        String sql;
        long startTime, endTime;

        public MyClickHouseSink(String sql) {
            this.sql = sql;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            conn = ClickHouseUtil.getConnection("master", 8123);
            psmt = conn.prepareStatement(sql);
            startTime = System.currentTimeMillis();
        }

        @Override
        public void close() throws Exception {
            super.close();
            endTime = System.currentTimeMillis();
            System.out.println("插入数据总用时：" + (endTime - startTime) + "ms");

            if (psmt != null) {
                try {
                    psmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                ClickHouseUtil.close();
            }
        }

        @Override
        public void invoke(String[] fields, Context context) throws Exception {
            psmt.setString(1, fields[0].trim());
            psmt.setString(2, fields[1].trim());
            psmt.setInt(3, Integer.parseInt(fields[2].trim()));

            psmt.execute();
        }
    }
}


