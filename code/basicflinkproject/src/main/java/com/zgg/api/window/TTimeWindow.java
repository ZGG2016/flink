package com.zgg.api.window;

import com.zgg.api.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inStream = env.socketTextStream("bigdata101",7777);
        DataStream<SensorReading> dataStream = inStream.map(line -> {
            String[] valArr = line.split(",");
            return new SensorReading(valArr[0], Long.parseLong(valArr[1]), Double.parseDouble(valArr[2]));
        });

        // 1. 增量聚合函数: 统计窗口内的数据个数
        // 每条数据到来就进行计算，保持一个简单的状态，等窗口到的时候，再输出
        DataStream<Integer> resStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(10))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return null;
                    }
                });

        // 2. 全量聚合函数: 统计窗口内的数据个数
        // 先把窗口所有数据收集起来，等窗口到的时候，再计算输出
//        DataStream<Tuple3<String,Long,Integer>> resStream = dataStream.keyBy("id")
//                .timeWindow(Time.seconds(10))
//                .apply(new WindowFunction<SensorReading, Tuple3<String,Long,Integer>, Tuple, TimeWindow>() {
//                    @Override
//                    public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String,Long,Integer>> out) throws Exception {
//                        String id = tuple.getField(0);
//                        Long endTime = window.getEnd();
//                        Integer count = IteratorUtils.toList(input.iterator()).size();
//                        out.collect(new Tuple3<>(id, endTime, count));
//                    }
//                });

        resStream.print();
        env.execute();
    }
}
