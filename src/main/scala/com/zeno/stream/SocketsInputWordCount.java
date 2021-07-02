package com.zeno.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class SocketsInputWordCount {

    public static void main(String[] args) throws Exception {

        /*
         * 在 host 系统上执行 nc -lk 8888 ，然后输出文本到 进行单词统计。
         */
        String host = "192.68.2.6";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(3);
        final DataStreamSource<String> initStream = env.socketTextStream(host,8888);

        DataStream<String> words = initStream.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String value, Collector<String> out) {
                String[] tokens = value.split(" ");
                for (String token : tokens){
                    if (token.length() > 0) {
                        out.collect(token);
                    }
                }
            }
        });

        DataStream<Tuple2<String, Integer>> pairStream = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            public Tuple2<String, Integer> map(String value) {
                return new Tuple2<String, Integer>(value,1);
            }
        });

        KeyedStream<Tuple2<String,Integer>, Tuple> keyedStream = pairStream.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String,Integer>> counts = keyedStream.sum(1);
        counts.print();
        env.execute("Socket WordCount");
    }
}
