package com.studio.flink.project;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Demo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String input = args[0];
        String output = args[1];

        DataStreamSource<String> stream = env.readTextFile(input);

        SingleOutputStreamOperator<String> result = stream.flatMap((String in, Collector<String> out) -> {
            String[] cols = in.split(",");
            String dateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(Long.valueOf(cols[4])*1000L));
            if (dateTime.compareTo("2017-11-25 00:00:00")>=0 && dateTime.compareTo("2017-12-04 00:00:00")<=0){
                String year = dateTime.substring(0, 10);
                String hour = dateTime.substring(11, 13);
                out.collect(in + "," + dateTime + "," + year + "," + hour);
            }
        }).returns(new TypeHint<String>() {
        });

        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(new Path(output),
                new SimpleStringEncoder<>("utf-8"))
                .build();
        result.addSink(streamingFileSink);

        env.execute();
    }
}
