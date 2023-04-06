package com.atguigu.flink.tuning;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.flink.source.MockSourceFunction;
import com.atguigu.flink.tuning.function.NewMidRichMapFunc;
import com.atguigu.flink.tuning.function.UvRichFilterFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.formats.sequencefile.SequenceFileWriterFactory;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.concurrent.TimeUnit;


public class UvDemo {
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


        // 按照mid分组，新老用户修正
        SingleOutputStreamOperator<JSONObject> jsonWithNewFlagDS = jsonobjDS
                .keyBy(data -> data.getJSONObject("common").getString("mid"))
                .map(new NewMidRichMapFunc());


//        SingleOutputStreamOperator<JSONObject> newDS = jsonWithNewFlagDS
//                .keyBy(data -> data.getJSONObject("common").getString("mid"))
//                .map(new NewMidRichMapFunc());

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
