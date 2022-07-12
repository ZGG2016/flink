package com.zgg.api.processfunction;

import com.zgg.api.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class KPFApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inStream = env.socketTextStream("bigdata101",7777);
        DataStream<SensorReading> dataStream = inStream.map(line -> {
            String[] valArr = line.split(",");
            return new SensorReading(valArr[0], Long.parseLong(valArr[1]), Double.parseDouble(valArr[2]));
        });

        // 检测 10s 内的温度连续上升，输出报警
        dataStream.keyBy("id")
                .process( new TempConsIncreWarning( 10 ) )
                .print();

        env.execute();
    }

    public static class TempConsIncreWarning extends KeyedProcessFunction<Tuple,SensorReading,String>{

        public Integer interval;

        public TempConsIncreWarning(Integer interval) {
            this.interval = interval;
        }

        // 保存上个温度值的状态
        public ValueState<Double> lastTempState;
        // 保存触发定时器的时间戳的状态
        public ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>(
                    "lasttemp-state",Double.class,Double.MIN_VALUE
            ));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>(
                    "time-state",Long.class
            ));
        }
        
        @Override
        public void processElement(SensorReading value, KeyedProcessFunction<Tuple, SensorReading, String>.Context ctx, Collector<String> out) throws Exception {
            Double curTemp = value.getTemperature();
            Double lastTemp = lastTempState.value();
            Long ts = timerTsState.value();
            // 来条数据，它的温度值是上升，且没有注册定时器
            if (curTemp > lastTemp && ts == null){
                // 注册定时器，且更新对应的状态
                ctx.timerService().registerProcessingTimeTimer( ctx.timerService().currentProcessingTime() + interval);
                timerTsState.update(ctx.timerService().currentProcessingTime() + interval);
            }
            // 来条数据，它的温度值是下降，且注册了定时器
            else if (curTemp <= lastTemp && ts != null){
                // 删除定时器，且清空对应的状态
                ctx.timerService().deleteProcessingTimeTimer(ts);
                timerTsState.clear();
            }
            // 来条数据，无论它的温度值是上升还是下降，都有更新这个状态
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Tuple, SensorReading, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，输出报警信息
            out.collect("传感器" + ctx.getCurrentKey().getField(0) + "温度值连续" + interval + "s上升");
            timerTsState.clear();
        }
        
        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }
}
