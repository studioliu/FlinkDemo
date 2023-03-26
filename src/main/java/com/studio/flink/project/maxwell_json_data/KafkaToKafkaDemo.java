package com.studio.flink.project.maxwell_json_data;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class KafkaToKafkaDemo {
    public static void main(String[] args) throws Exception {
        // TODO: 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 生产环境中设置为kafka主题的分区数

        // TODO: 2.消费Kafka ods 主题的数据创建流
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("master:9092")
                .setTopics("ods")
                .setGroupId("Consumer_Group01")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStreamSource<String> streamSource = env.fromSource(kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "kafkaSource");

        // TODO: 3.过滤掉非JSON格式的数据 & 将每行数据转换为JSON对象
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = streamSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyTag, s);
                }
            }
        });
        // 获取侧输出流脏数据并打印
        DataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDS.print("Dirty>>>>>>>>>>>>");

        // TODO: 4.按照 table 分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(e -> e.getString("table"));

        OutputTag<JSONObject> userInfoTag = new OutputTag<JSONObject>("user_info") {
        };
        SingleOutputStreamOperator<JSONObject> processDS = keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                if ("user_info".equals(context.getCurrentKey())) {
                    context.output(userInfoTag, jsonObject);
                } else {
                    collector.collect(jsonObject);
                }
            }
        });

        DataStream<JSONObject> userInfoDS = processDS.getSideOutput(userInfoTag);

        // TODO: 5.将数据写入对应的主题
        userInfoDS.map(JSONObject::toString).addSink(
                new FlinkKafkaProducer<>("master:9092", "dwd_user_info", new SimpleStringSchema())
        );
        processDS.map(JSONObject::toString).addSink(
                new FlinkKafkaProducer<String>("master:9092", "dwd_other", new SimpleStringSchema())
        );

        // TODO: 6.启动任务
        env.execute("KafkaToKafkaDemo");
    }
}


/*
{"database":"gmall","table":"user_info","type":"insert","ts":1678278951,"xid":10892,"xoffset":0,"data":{"id":1,"login_name":"5g1fjio06","nick_name":null,"passwd":null,"name":null,"phone_num":"13956414426","email":"5g1fjio06@0355.net","head_img":null,"user_level":"1","birthday":"1994-06-14","gender":null,"create_time":"2020-06-14 20:35:49","operate_time":null,"status":null}}
{"database":"gmall","table":"user_info","type":"insert","ts":1678278951,"xid":10892,"xoffset":1,"data":{"id":2,"login_name":"l9udbaph9tq9","nick_name":null,"passwd":null,"name":null,"phone_num":"13193332735","email":"l9udbaph9tq9@sohu.com","head_img":null,"user_level":"1","birthday":"1988-11-14","gender":null,"create_time":"2020-06-14 20:35:49","operate_time":null,"status":null}}
{"database":"gmall","table":"user_info","type":"update","ts":1678278952,"xid":10892,"xoffset":200,"data":{"id":1,"login_name":"5g1fjio06","nick_name":"阿坚","passwd":null,"name":"乐坚","phone_num":"13956414426","email":"5g1fjio06@0355.net","head_img":null,"user_level":"1","birthday":"1994-06-14","gender":"M","create_time":"2020-06-14 20:35:49","operate_time":null,"status":null},"old":{"nick_name":null,"name":null,"gender":null}}
{"database":"gmall","table":"user_info","type":"update","ts":1678278952,"xid":10892,"xoffset":201,"data":{"id":2,"login_name":"l9udbaph9tq9","nick_name":"娟娟","passwd":null,"name":"孟娟","phone_num":"13193332735","email":"l9udbaph9tq9@sohu.com","head_img":null,"user_level":"1","birthday":"1988-11-14","gender":"F","create_time":"2020-06-14 20:35:49","operate_time":null,"status":null},"old":{"nick_name":null,"name":null,"gender":null}}
{"database":"gmall","table":"favor_info","type":"delete","ts":1678278956,"xid":10892,"xoffset":400,"data":{"id":1633444394122846210,"user_id":101,"sku_id":33,"spu_id":null,"is_cancel":"0","create_time":"2020-06-14 20:27:45","cancel_time":null}}
{"database":"gmall","table":"favor_info","type":"delete","ts":1678278956,"xid":10892,"xoffset":401,"data":{"id":1633444394122846211,"user_id":53,"sku_id":17,"spu_id":null,"is_cancel":"0","create_time":"2020-06-14 20:27:45","cancel_time":null}}
=============================================
{
    "database": "gmall",
    "table": "user_info",
    "type": "insert",
    "ts": 1678278951,
    "xid": 10892,
    "xoffset": 0,
    "data": {
        "id": 1,
        "login_name": "5g1fjio06",
        "nick_name": null,
        "passwd": null,
        "name": null,
        "phone_num": "13956414426",
        "email": "5g1fjio06@0355.net",
        "head_img": null,
        "user_level": "1",
        "birthday": "1994-06-14",
        "gender": null,
        "create_time": "2020-06-14 20:35:49",
        "operate_time": null,
        "status": null
    }
}
---------------------------------------------
{
    "database": "gmall",
    "table": "user_info",
    "type": "update",
    "ts": 1678278952,
    "xid": 10892,
    "xoffset": 200,
    "data": {
        "id": 1,
        "login_name": "5g1fjio06",
        "nick_name": "阿坚",
        "passwd": null,
        "name": "乐坚",
        "phone_num": "13956414426",
        "email": "5g1fjio06@0355.net",
        "head_img": null,
        "user_level": "1",
        "birthday": "1994-06-14",
        "gender": "M",
        "create_time": "2020-06-14 20:35:49",
        "operate_time": null,
        "status": null
    },
    "old": {
        "nick_name": null,
        "name": null,
        "gender": null
    }
}
---------------------------------------------
{
    "database": "gmall",
    "table": "favor_info",
    "type": "delete",
    "ts": 1678278956,
    "xid": 10892,
    "xoffset": 400,
    "data": {
        "id": 1633444394122846210,
        "user_id": 101,
        "sku_id": 33,
        "spu_id": null,
        "is_cancel": "0",
        "create_time": "2020-06-14 20:27:45",
        "cancel_time": null
    }
}
 */