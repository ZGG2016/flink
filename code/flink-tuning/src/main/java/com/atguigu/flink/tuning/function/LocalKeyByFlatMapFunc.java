package com.atguigu.flink.tuning.function;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalKeyByFlatMapFunc extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> implements CheckpointedFunction {

    //Checkpoint 时为了保证 Exactly Once，将 buffer 中的数据保存到该 ListState 中
    private ListState<Tuple2<String, Long>> listState;

    //本地 buffer，存放 local 端缓存的 mid 的 count 信息
    private HashMap<String, Long> localBuffer;

    //缓存的数据量大小，即：缓存多少数据再向下游发送
    private int batchSize;

    //计数器，获取当前批次接收的数据量
    private AtomicInteger currentSize;


    //构造器，批次大小传参
    public LocalKeyByFlatMapFunc(int batchSize) {
        this.batchSize = batchSize;
    }


    @Override
    public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, Long>> out) throws Exception {
        // 1、将新来的数据添加到 buffer 中,本地聚合
        Long count = localBuffer.getOrDefault(value.f0, 0L);
        localBuffer.put(value.f0, count + 1);

        // 2、如果到达设定的批次，则将 buffer 中的数据发送到下游
        if (currentSize.incrementAndGet() >= batchSize) {
            // 2.1 遍历 Buffer 中数据，发送到下游
            for (Map.Entry<String, Long> midAndCount : localBuffer.entrySet()) {
                out.collect(Tuple2.of(midAndCount.getKey(), midAndCount.getValue()));
            }

            // 2.2 Buffer 清空，计数器清零
            localBuffer.clear();
            currentSize.set(0);
        }

    }


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // 将 buffer 中的数据保存到状态中，来保证 Exactly Once
        listState.clear();
        for (Map.Entry<String, Long> midAndCount : localBuffer.entrySet()) {
            listState.add(Tuple2.of(midAndCount.getKey(), midAndCount.getValue()));
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 从状态中恢复 buffer 中的数据
        listState = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<Tuple2<String, Long>>(
                        "localBufferState",
                        Types.TUPLE(Types.STRING, Types.LONG)
                )
        );
        localBuffer = new HashMap();
        if (context.isRestored()) {
            // 从状态中恢复数据到 buffer 中
            for (Tuple2<String, Long> midAndCount : listState.get()) {
                // 如果出现 pv != 0,说明改变了并行度，ListState 中的数据会被均匀分发到新的 subtask中
                // 单个 subtask 恢复的状态中可能包含多个相同的 mid 的 count数据
                // 所以每次先取一下buffer的值，累加再put
                long count = localBuffer.getOrDefault(midAndCount.f0, 0L);
                localBuffer.put(midAndCount.f0, count + midAndCount.f1);
            }
            // 从状态恢复时，默认认为 buffer 中数据量达到了 batchSize，需要向下游发
            currentSize = new AtomicInteger(batchSize);
        } else {
            currentSize = new AtomicInteger(0);
        }

    }
}
