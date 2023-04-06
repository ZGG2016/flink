package com.atguigu.flink.tuning;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.flink.source.MockSourceFunction;
import com.atguigu.flink.tuning.bean.MidCountAndWindowEnd;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Random;
import java.util.concurrent.TimeUnit;


public class SkewDemo2 {
    public static void main(String[] args) throws Exception {

//        Configuration conf = new Configuration();
//        conf.set(RestOptions.ENABLE_FLAMEGRAPH, true);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
//        env.setParallelism(1);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();

        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(3), CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://hadoop1:8020/flink-tuning/ck");
//        checkpointConfig.setCheckpointStorage("file:///F:/flink-tuning/test/ck");
        checkpointConfig.setMinPauseBetweenCheckpoints(TimeUnit.SECONDS.toMillis(3));
        checkpointConfig.setTolerableCheckpointFailureNumber(5);
        checkpointConfig.setCheckpointTimeout(TimeUnit.MINUTES.toMillis(1));
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        SingleOutputStreamOperator<JSONObject> jsonobjDS = env
                .addSource(new MockSourceFunction())
                .map(data -> JSONObject.parseObject(data));

        // 过滤出 页面数据,转换成 (mid,1L)
        SingleOutputStreamOperator<Tuple2<String, Long>> pageMidTuple = jsonobjDS
                .filter(data -> StringUtils.isEmpty(data.getString("start")))
                .map(r -> Tuple2.of(r.getJSONObject("common").getString("mid"), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));


        // 按照mid分组，统计每10s,各mid出现的次数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        boolean isTwoPhase = parameterTool.getBoolean("two-phase", true);
        int randomNum = parameterTool.getInt("random-num", 5);

        if (!isTwoPhase) {
            pageMidTuple
                    .keyBy(r -> r.f0)
                    .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                    .reduce((value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1))
                    .print().setParallelism(1);
        } else {
            // 拼接随机数打散，第一次聚合（窗口聚合）
            SingleOutputStreamOperator<Tuple3<String, Long, Long>> firstAgg = pageMidTuple
                    .map(new MapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
                        Random random = new Random();
                        @Override
                        public Tuple2<String, Long> map(Tuple2<String, Long> value) throws Exception {
                            return Tuple2.of(value.f0 + "-" + random.nextInt(randomNum), 1L);
                        }
                    }) // mid拼接随机数
                    .keyBy(r -> r.f0) // 第一次按照 "mid|随机数" 分组
                    .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                    .reduce(
                            (value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1),
                            new ProcessWindowFunction<Tuple2<String, Long>, Tuple3<String, Long, Long>, String, TimeWindow>() {
                                @Override
                                public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple3<String, Long, Long>> out) throws Exception {
                                    Tuple2<String, Long> midAndCount = elements.iterator().next();
                                    long windowEndTs = context.window().getEnd();
                                    out.collect(Tuple3.of(midAndCount.f0, midAndCount.f1, windowEndTs));
                                }
                            }
                    );// 窗口聚合（第一次聚合），加上窗口结束时间的标记，方便第二次聚合汇总

            // 按照原来的 key和windowEnd分组，第二次聚合
            firstAgg
                    .map(new MapFunction<Tuple3<String,Long,Long>, Tuple3<String,Long,Long>>() {
                        @Override
                        public Tuple3<String, Long, Long> map(Tuple3<String, Long, Long> value) throws Exception {
                            String originKey = value.f0.split("-")[0];
                            return Tuple3.of(originKey,value.f1 ,value.f2);
                        }
                    }) // 去掉 拼接的随机数
                    .keyBy(new KeySelector<Tuple3<String, Long, Long>, Tuple2<String, Long>>() {
                        @Override
                        public Tuple2<String, Long> getKey(Tuple3<String, Long, Long> value) throws Exception {
                            return Tuple2.of(value.f0, value.f2);
                        }
                    }) // 按照 原来的 key和 窗口结束时间 分组
                    .reduce((value1, value2) -> Tuple3.of(value1.f0, value1.f1 + value2.f1, value1.f2)) // 第二次真正聚合
                    .print().setParallelism(1);
        }

        env.execute();
    }
}
