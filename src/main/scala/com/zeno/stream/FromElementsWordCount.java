package com.zeno.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class FromElementsWordCount {

    public static void main(String[] args) throws Exception {

        // StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("192.168.2.5",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(3);

        final DataStreamSource<String> initStream = env.fromElements("C:\\Users\\zeno\\.m2\\settings.xml\\Users\\zeno\\.m2");

        DataStream<Tuple2<String,Integer>> words = initStream.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            public void flatMap(String value, Collector<Tuple2<String,Integer>> out) {
                String[] tokens = value.split("\\\\");
                for (String token : tokens){
                    if (token.length() > 0) {
                        out.collect(new Tuple2<String,Integer>(token, 1));
                    }
                }
            }
        });

        KeyedStream<Tuple2<String,Integer>,String> pairStream = words.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            public String getKey(Tuple2<String, Integer> value) {
                return value.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String,Integer>> counts = pairStream.sum(1);
        counts.print();
        env.execute("Streaming WordCount");
    }
}
