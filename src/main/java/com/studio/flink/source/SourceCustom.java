package com.studio.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceCustom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //有了自定义的source function，调用addSource方法
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        stream.print("SourceCustom");

        env.execute();
    }
}

