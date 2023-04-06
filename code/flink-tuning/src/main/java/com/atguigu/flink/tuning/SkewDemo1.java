package com.atguigu.flink.tuning;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.flink.source.MockSourceFunction;
import com.atguigu.flink.tuning.function.LocalKeyByFlatMapFunc;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;


public class SkewDemo1 {
    public static void main(String[] args) throws Exception {

//        Configuration conf = new Configuration();
//        conf.set(RestOptions.ENABLE_FLAMEGRAPH, true);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setParallelism(1);
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


        // 按照mid分组，统计每个mid出现的次数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        boolean isLocalKeyby = parameterTool.getBoolean("local-keyby", false);
        if (!isLocalKeyby) {
            pageMidTuple
                    .keyBy(r -> r.f0)
                    .reduce((value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1))
                    .print().setParallelism(1);
        } else {
            pageMidTuple
                    .flatMap(new LocalKeyByFlatMapFunc(10000)) // 实现 localkeyby的功能
                    .keyBy(r -> r.f0)
                    .reduce((value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1))
                    .print().setParallelism(1);
        }

        env.execute();
    }
}
