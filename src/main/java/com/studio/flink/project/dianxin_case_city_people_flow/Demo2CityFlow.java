package com.studio.flink.project.dianxin_case_city_people_flow;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * 实时统计每个城市的人流量
 * 需要对手机号去重
 * -----------------------
 * 手机号(通过md5加密的)--脱敏，网格编号，城市编号，区县编号，停留时间，进入时间，离开时间，……
 * D55433A437AEC8D8D3DB2BCA56E9E64392A9D93C,117210031795040,83401,8340104,301,20180503190539,20180503233517,20180503
 */
public class Demo2CityFlow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取Kafka中的数据
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer_group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer<String>("dianxin", new SimpleStringSchema(), properties));

        // 取出城市编码和手机号
        SingleOutputStreamOperator<Tuple2<String, String>> mapDS = kafkaDS.map(data -> {
            String[] fields = data.split(",");
            String mdn = fields[0];
            String city = fields[2];
            return Tuple2.of(city, mdn);
        }).returns(new TypeHint<Tuple2<String, String>>() {
        });

        // 按照城市分组
        KeyedStream<Tuple2<String, String>, String> keyedStream = mapDS.keyBy(e -> e.f0);

        // 统计人流量
        SingleOutputStreamOperator<Tuple2<String, Integer>> cityCountDS = keyedStream
                .process(new KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, Integer>>() {
                    // map 状态 -- 去重，使用map的key保存手机号，map的value不用
                    MapState<String, Integer> mapState = null;
                    ReducingState<Integer> reduceState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        RuntimeContext runtimeContext = getRuntimeContext();

                        // 用于手机号去重的状态
                        MapStateDescriptor<String, Integer> mapStateDesc = new MapStateDescriptor<>("mdns", String.class, Integer.class);
                        mapState = runtimeContext.getMapState(mapStateDesc);

                        // 用于统计人流量的状态
                        ReducingStateDescriptor<Integer> reduceStateDesc = new ReducingStateDescriptor<>("count", new ReduceFunction<Integer>() {
                            @Override
                            public Integer reduce(Integer integer, Integer t1) throws Exception {
                                return integer + t1;
                            }
                        }, Integer.class);
                        reduceState = runtimeContext.getReducingState(reduceStateDesc);
                    }

                    @Override
                    public void processElement(Tuple2<String, String> stringStringTuple2, KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, Integer>>.Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        //1、判断当前手机号是否出现过
                        //如果手机号出现过，不需要做任务处理
                        //如果没有出现过，在之前的统计基础上加1
                        String city = stringStringTuple2.f0;
                        String mdn = stringStringTuple2.f1;
                        if (!mapState.contains(mdn)) {
                            // 将当前手机号保存到状态中
                            mapState.put(mdn, 1);

                            // 人流量加1
                            reduceState.add(1);

                            // 获取最新的人流量
                            Integer count = reduceState.get();

                            // 将数据发送到下游
                            collector.collect(Tuple2.of(city, count));
                        }
                    }
                });

        cityCountDS.print("Result>>>>>>>>>>");

        env.execute();
    }
}

