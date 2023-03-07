package com.studio.flink.project;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class KafkaToKafkaDemo {
    public static void main(String[] args) throws Exception {

        // TODO: 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 生产环境中设置为kafka主题的分区数

        // TODO: 2.消费Kafka ods 主题的数据创建流
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "Test");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer<String>(
                "ods",
                new SimpleStringSchema(),
                properties
        ));

        // TODO: 3.过滤掉非JSON格式的数据 & 将每行数据转换为JSON对象
        OutputTag<String> dirtyTag = new OutputTag<>("Dirty", TypeInformation.of(new TypeHint<String>() {
        }));
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyTag, s);
                }
            }
        });
        // 获取侧输出流脏数据并打印
        DataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDS.print("Dirty>>>>>>>>>>>>");

        // TODO: 4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(
                json -> json.getJSONObject("common").getString("mid")
        );
        SingleOutputStreamOperator<String> LogDS = keyedStream.map(JSONObject::toString);

        // TODO: 5.将数据写入对应的主题
        LogDS.addSink(new FlinkKafkaProducer<String>("master:9092", "dwd", new SimpleStringSchema()));

        // TODO: 6.启动任务
        env.execute("Test");
    }
}

/*
 测试数据
{"common":{"mid":1},"info":{"user_name":"Mary","cnt":3}}
{"common":{"mid":3},"info":{"user_name":"Mary","cnt":1}}
{"common":{"mid":1},"info":{"user_name":"Alice","cnt":1}}
{"common":{"mid":2},"info":{"user_name":"Alice","cnt":3}}
{"common":{"mid":1},"info":{"user_name":"Alice","cnt":2}}
{"common":{"mid":4},"info":{"user_name":"Bob","cnt":6}}
{"common":{"mid":1},"info":{"user_name":"Bob","cnt":8}}
 */