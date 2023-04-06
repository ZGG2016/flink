package com.zgg.api.processfunction;

import com.zgg.api.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class KKeyedProcessFunction {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inStream = env.socketTextStream("bigdata101",7777);
        DataStream<SensorReading> dataStream = inStream.map(line -> {
            String[] valArr = line.split(",");
            return new SensorReading(valArr[0], Long.parseLong(valArr[1]), Double.parseDouble(valArr[2]));
        });

        dataStream.keyBy("id")
                  .process( new MyKeyedProcess() )
                  .print();

        env.execute();
    }
    // 注意 KeyedProcessFunction 第一个泛型的类型是 Tuple
    public static class MyKeyedProcess extends KeyedProcessFunction<Tuple,SensorReading, Tuple3<String, Long, String>>{

        // 定义一个状态，放'当前的处理时间'，方便在注销定时器时使用
        private ValueState<Long> curProcTimeState;

        @Override
        public void open(Configuration parameters) throws Exception {
            curProcTimeState = getRuntimeContext().getState( new ValueStateDescriptor<Long>("time-state",Long.class));
        }

        // 这里使用的事件时间语义
        @Override
        public void processElement(SensorReading value, KeyedProcessFunction<Tuple, SensorReading, Tuple3<String, Long, String>>.Context ctx, Collector<Tuple3<String, Long, String>> out) throws Exception {
            // 获取正在被处理元素的时间，如果是事件时间语义，那么就返回null
            Long timestamp = ctx.timestamp();
            // 正在被处理元素的 key
            Tuple currentKey = ctx.getCurrentKey();
            out.collect(new Tuple3<>(value.getId(), timestamp, currentKey.getField(0)));

            // 获取当前的处理时间
            long curProcTime = ctx.timerService().currentProcessingTime();
            curProcTimeState.update(curProcTime + 5000L);
            // 当前的处理时间延迟5秒设置一个定时器
            ctx.timerService().registerProcessingTimeTimer(curProcTime + 5000L);
            // 删除之前注册的事件时间定时器，如果没有此时间戳的定时器，则不执行。
            // 删除在这个指定的时间戳注册的定时器
            ctx.timerService().deleteProcessingTimeTimer(curProcTimeState.value());
        }

        // 定时器触发时做的一些事情
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Tuple, SensorReading, Tuple3<String, Long, String>>.OnTimerContext ctx, Collector<Tuple3<String, Long, String>> out) throws Exception {
            // 触发定时器的key
            Tuple currentKey = ctx.getCurrentKey();
            // 这个时间戳是触发定时器的时间戳
            out.collect(new Tuple3<>("定时器内的输出",timestamp,currentKey.getField(0)));
        }

        @Override
        public void close() throws Exception {
            curProcTimeState.clear();
        }
    }
}
