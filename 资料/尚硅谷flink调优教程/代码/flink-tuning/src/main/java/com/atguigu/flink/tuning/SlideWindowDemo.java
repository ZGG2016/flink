package com.atguigu.flink.tuning;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.flink.source.MockSourceFunction;
import com.atguigu.flink.tuning.function.NewMidRichMapFunc;
import com.atguigu.flink.tuning.function.SplitTumbleWindowPAWF;
import com.atguigu.flink.tuning.function.SplitWindowAggFunction;
import com.atguigu.flink.tuning.function.UvRichFilterFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;


public class SlideWindowDemo {
    public static void main(String[] args) throws Exception {

//        Configuration conf = new Configuration();
//        conf.set(RestOptions.ENABLE_FLAMEGRAPH, true);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(0, 0));
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


        // 按照mid分组，新老用户修正
        SingleOutputStreamOperator<JSONObject> jsonWithNewFlagDS = jsonobjDS
                .keyBy(data -> data.getJSONObject("common").getString("mid"))
                .map(new NewMidRichMapFunc());

        // 过滤出 页面数据
        SingleOutputStreamOperator<JSONObject> pageObjDS = jsonWithNewFlagDS.filter(data -> StringUtils.isEmpty(data.getString("start")));


        // 按照mid分组，过滤掉不是今天第一次访问的数据
        SingleOutputStreamOperator<JSONObject> uvDS = pageObjDS
                .keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"))
                .filter(new UvRichFilterFunction());

        // 统计最近1小时的uv，1秒更新一次
        SingleOutputStreamOperator<Long> uvOneDS = uvDS
                .map(r -> 1L);


        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        boolean isSlidingSplit = parameterTool.getBoolean("sliding-split", false);

        if (isSlidingSplit) {
            uvOneDS
                    .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                    .reduce(
                            (value1, value2) -> value1 + value2,
                            new SplitTumbleWindowPAWF()
                    )
                    .keyBy(r -> 1)
                    .process(new SplitWindowAggFunction()).setParallelism(1)
                    .print().setParallelism(1);


        } else {
            uvOneDS
                    .windowAll(SlidingProcessingTimeWindows.of(Time.hours(1), Time.seconds(1)))
                    .reduce(
                            (value1, value2) -> value1 + value2,
                            new SplitTumbleWindowPAWF())
                    .print().setParallelism(1);
        }

        env.execute();
    }
}
