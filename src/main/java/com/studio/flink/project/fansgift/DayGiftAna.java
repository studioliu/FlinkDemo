package com.studio.flink.project.fansgift;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Job: 实时统计主播粉丝送礼物排行榜
 * @desc 贡献日榜计算
 */
public class DayGiftAna {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.getConfig().setAutoWatermarkInterval(1000L);    //BoundedOutOfOrdernessWatermarks定时提交Watermark的间隔
        env.setStateBackend(new RocksDBStateBackend("hdfs://master:9000/dayGiftAna"));

        // 从kafka读取数据，转换成GiftRecord对象
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "master:9092,slave1:9092");
//        properties.setProperty("group.id", "gift");
//        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("flinktopic",
//                new SimpleStringSchema(),
//                properties).setStartFromLatest());

        // 使用Socket测试
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<FansGiftResult> fansGiftResult = dataStream.map(s -> {
                    String[] fields = s.split(",");
                    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    long giftTime = LocalDateTime.parse(fields[3], dtf).toInstant(ZoneOffset.UTC).toEpochMilli();
                    return new GiftRecord(fields[0], fields[1], Integer.parseInt(fields[2]), giftTime);
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<GiftRecord>forBoundedOutOfOrderness(Duration.ofMinutes(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<GiftRecord>() {
                                    @Override
                                    public long extractTimestamp(GiftRecord giftRecord, long l) {
                                        return giftRecord.giftTime;
                                    }
                                })
                ).keyBy(r -> r.hostId + "_" + r.fansId)
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
//                .allowedLateness(Time.seconds(2))
                .aggregate(new WindowGiftRecordAgg(), new AllWindowGiftRecordAgg());
        // 打印结果测试
        fansGiftResult.print("fansGiftResult>>>>>>>>>>>");

        // 将结果存入ES
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("master", 9200));
        ElasticsearchSink.Builder<FansGiftResult> esBuilder = new ElasticsearchSink.Builder<FansGiftResult>(
                httpHosts,
                new ElasticsearchSinkFunction<FansGiftResult>() {
                    @Override
                    public void process(FansGiftResult fansGiftResult, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        Map<String, Object> data = new HashMap<>();
                        data.put("hostId", fansGiftResult.hostId);
                        data.put("fansId", fansGiftResult.fansId);
                        data.put("giftCount", fansGiftResult.giftCount);
                        data.put("windowEnd", fansGiftResult.windowEnd);
                        IndexRequest esRequest = Requests.indexRequest()
                                .index("daygiftanalyze")
                                .id("" + System.currentTimeMillis())
                                .source(data);
                    }
                });
        esBuilder.setBulkFlushMaxActions(1);    // 每一条记录直接写入到ES，不做本地缓存
        esBuilder.setBulkFlushBackoffRetries(3);    // 写入失败后的重试次数
//        esBuilder.setFailureHandler(new ActionRequestFailureHandler() {     // 失败处理策略
//            @Override
//            public void onFailure(ActionRequest actionRequest, Throwable throwable, int i, RequestIndexer requestIndexer) throws Throwable {
////                requestIndexer.add();   // 重新加入请求
//            }
//        });
        ElasticsearchSink<FansGiftResult> esSink = esBuilder.build();
        fansGiftResult.addSink(esSink);


        env.execute("DayGiftAna");
    }

    // 在每个子任务中将窗口期内的礼物进行累计合并
    // 增加状态后端
    public static class WindowGiftRecordAgg implements AggregateFunction<GiftRecord, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(GiftRecord giftRecord, Long aLong) {
            return aLong + giftRecord.giftCount;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    // 对窗口期内的所有子任务进行窗口聚合操作
    public static class AllWindowGiftRecordAgg extends RichWindowFunction<Long, FansGiftResult, String, TimeWindow> {
        ValueState<FansGiftResult> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(
                    new ValueStateDescriptor<FansGiftResult>("AllWindowGiftRecordAgg", FansGiftResult.class)
            );
        }

        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<Long> iterable, Collector<FansGiftResult> collector) throws Exception {
            String[] splitKey = s.split("_");
            String hostId = splitKey[0];
            String fansId = splitKey[1];
            Long giftCount = iterable.iterator().next();
            long windowEnd = timeWindow.getEnd();
            FansGiftResult fansGiftResult = new FansGiftResult(hostId, fansId, giftCount, windowEnd);
            collector.collect(fansGiftResult);
            valueState.update(fansGiftResult);
        }
    }
}
