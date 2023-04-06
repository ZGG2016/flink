package com.atguigu.flink.tuning;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.flink.source.MockSourceFunction;
import com.atguigu.flink.tuning.function.NewMidRichMapFunc;
import com.atguigu.flink.tuning.function.UvRichFilterFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;


public class UidDemo {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStateBackend(new HashMapStateBackend());
        env.getConfig().enableObjectReuse();
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(3), CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://hadoop1:8020/flink-tuning/ck");
        checkpointConfig.setMinPauseBetweenCheckpoints(TimeUnit.SECONDS.toMillis(3));
        checkpointConfig.setTolerableCheckpointFailureNumber(5);
        checkpointConfig.setCheckpointTimeout(TimeUnit.MINUTES.toMillis(1));
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        SingleOutputStreamOperator<JSONObject> jsonobjDS = env
                .addSource(new MockSourceFunction()).uid("mock-source").name("mock-source")
                .map(data -> JSONObject.parseObject(data)).uid("parsejson-map").name("parsejson-map");


        // 按照mid分组，新老用户修正
        SingleOutputStreamOperator<JSONObject> jsonWithNewFlagDS = jsonobjDS
                .keyBy(data -> data.getJSONObject("common").getString("mid"))
                .map(new NewMidRichMapFunc()).uid("fixNewMid-map").name("fixNewMid-map");

//        SingleOutputStreamOperator<JSONObject> newDS = jsonWithNewFlagDS
//                .keyBy(data -> data.getJSONObject("common").getString("mid"))
//                .map(new NewMidRichMapFunc()).uid("add-fixNewMid-map").name("add-fixNewMid-map");

        // 过滤出 页面数据
        SingleOutputStreamOperator<JSONObject> pageObjDS = jsonWithNewFlagDS
                .filter(data -> StringUtils.isEmpty(data.getString("start")))
                .uid("page-filter").name("page-filter");

        // 按照mid分组，过滤掉不是今天第一次访问的数据
        SingleOutputStreamOperator<JSONObject> uvDS = pageObjDS
                .keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"))
                .filter(new UvRichFilterFunction()).uid("firstMid-filter").name("firstMid-filter");

        // 实时统计uv
        uvDS
                .map(r -> Tuple3.of("uv", r.getJSONObject("common").getString("mid"), 1L)).uid("uvAndOne-map").name("uvAndOne-map")
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                .keyBy(r -> r.f0)
                .reduce((value1, value2) -> Tuple3.of("uv", value2.f1, value1.f2 + value2.f2)).uid("uv-reduce").name("uv-reduce")
                .print().uid("uv-print").name("uv-print").setParallelism(1);



        env.execute();
    }
}
