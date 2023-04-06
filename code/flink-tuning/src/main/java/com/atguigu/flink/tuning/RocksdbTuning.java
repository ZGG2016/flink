package com.atguigu.flink.tuning;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.flink.source.MockSourceFunction;
import com.atguigu.flink.tuning.function.NewMidRichMapFunc;
import com.atguigu.flink.tuning.function.UvRichFilterFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class RocksdbTuning {
    public static void main(String[] args) throws Exception {

//        Configuration conf = new Configuration();
//        conf.set(RestOptions.ENABLE_FLAMEGRAPH, true);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setParallelism(1);
        env.disableOperatorChaining();

        EmbeddedRocksDBStateBackend embeddedRocksDBStateBackend = new EmbeddedRocksDBStateBackend();
//        embeddedRocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        env.setStateBackend(embeddedRocksDBStateBackend);
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(3), CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://hadoop1:8020/flink-tuning/ck");
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

        // 实时统计uv
        uvDS
                .map(r -> Tuple3.of("uv", r.getJSONObject("common").getString("mid"), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                .keyBy(r -> r.f0)
                .reduce((value1, value2) -> Tuple3.of("uv", value2.f1, value1.f2 + value2.f2))
                .print().setParallelism(1);


        env.execute();
    }
}
