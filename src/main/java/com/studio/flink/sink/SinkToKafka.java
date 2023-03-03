package com.studio.flink.sink;

import com.studio.flink.source.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class SinkToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从kafka读取数据
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        DataStreamSource<String> kafkaStream = env.addSource(
                new FlinkKafkaConsumer<>("clicks", new SimpleStringSchema(), properties)
        );

        // 2.转换处理
        SingleOutputStreamOperator<String> result = kafkaStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                String[] fields = value.split(",");
                return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim())).toString();
            }
        });

        // 3.结果写入kafka
        result.addSink(new FlinkKafkaProducer<>("master:9092", "events", new SimpleStringSchema()));

        env.execute();
    }
}

