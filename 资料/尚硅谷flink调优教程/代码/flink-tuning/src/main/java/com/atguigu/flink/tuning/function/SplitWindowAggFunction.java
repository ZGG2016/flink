package com.atguigu.flink.tuning.function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;


public class SplitWindowAggFunction extends KeyedProcessFunction<Integer, Tuple3<String, String, Long>, Tuple3<String, String, Long>> {
    ValueState<List<Tuple3<String, String, Long>>> splitListState;
    List<Tuple3<String, String, Long>> splitList;
    int splitNum;
    Long windowUvCount;
    Tuple3<String, String, Long> windowAggResult;

    @Override
    public void open(Configuration parameters) throws Exception {
        splitListState = getRuntimeContext()
                .getState(
                        new ValueStateDescriptor<List<Tuple3<String, String, Long>>>(
                                "splitListState",
                                Types.LIST(Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)),
                                new ArrayList<Tuple3<String, String, Long>>()
                        )
                );
        // 需求：60分钟的窗口，1秒的滑动 ===》 分片数=60分钟/1秒
        splitNum = 60 * 60;
        windowAggResult = new Tuple3<>();
    }

    @Override
    public void processElement(Tuple3<String, String, Long> value, Context ctx, Collector<Tuple3<String, String, Long>> out) throws Exception {
        // 每个时间分片的结果来，先清空统计值
        windowAggResult.f2 = 0L;
        // 将新的时间分片结果添加到List，删除第一个过期的时间分片
        splitList = splitListState.value();
        splitList.add(value);
        if (splitList.size() >= splitNum) {
            if (splitList.size() == (splitNum + 1)) {
                splitList.remove(0);
            }
            // 对时间分片聚合(复用Tuple3对象，减小开销)
            for (int i = 0; i < splitList.size(); i++) {
                // 累加时间分片的统计结果
                windowAggResult.f2 += splitList.get(i).f2;
                // windowStart取第一个元素的 开始时间
                if (i == 0) {
                    windowAggResult.f0 = splitList.get(i).f0;
                }
                // windowEnd取最后一个元素的 结束时间
                if (i == (splitList.size() - 1)) {
                    windowAggResult.f1 = splitList.get(i).f1;
                }
            }
            out.collect(windowAggResult);
        }

    }

}


