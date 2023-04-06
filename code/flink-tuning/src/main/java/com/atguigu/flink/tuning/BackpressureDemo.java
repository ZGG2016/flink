package com.atguigu.flink.tuning;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;


public class BackpressureDemo {
    public static void main(String[] args) throws Exception {

//        Configuration conf = new Configuration();
//        conf.set(RestOptions.ENABLE_FLAMEGRAPH, true);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
//        env.setParallelism(1);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();

        env
                .addSource(new SourceFunction<String>() {

                    volatile boolean flag = true;

                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        while(flag){
                            ctx.collect("a b c");
                        }
                    }

                    @Override
                    public void cancel() {
                        flag = false;
                    }
                })
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String line, Collector<String> out) throws Exception {
                        for (String word : line.split(" ")) {
                            for (int i = 0; i < Integer.MAX_VALUE; i++) {
                                word += i;
                            }
                            out.collect(word);
                        }
                    }
                })
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String word) throws Exception {

                        return Tuple2.of(word, 1L);
                    }
                })
                .keyBy(r -> r.f0)
                .sum(1)
                .print();

        env.execute();
    }
}
