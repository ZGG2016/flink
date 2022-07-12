package com.zgg.api.window;

import com.zgg.api.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class CCountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inStream = env.socketTextStream("bigdata101",7777);
        DataStream<SensorReading> dataStream = inStream.map(line -> {
            String[] valArr = line.split(",");
            return new SensorReading(valArr[0], Long.parseLong(valArr[1]), Double.parseDouble(valArr[2]));
        });

        // 计算窗口内数据的平均值
        // 增量聚合函数
        DataStream<Double> resStream = dataStream.keyBy("id")
                .countWindow(10,2)
                        .aggregate(new AggregateFunction<SensorReading, Tuple2<Double,Integer>, Double>() {
                            @Override
                            public Tuple2<Double, Integer> createAccumulator() {
                                return new Tuple2<Double,Integer>(0.0,0);
                            }

                            @Override
                            public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> accumulator) {
                                return new Tuple2<Double,Integer>(value.getTemperature()+accumulator.f0, accumulator.f1+1);
                            }

                            @Override
                            public Double getResult(Tuple2<Double, Integer> accumulator) {
                                return accumulator.f0 / accumulator.f1;
                            }

                            @Override
                            public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                                return new Tuple2<Double, Integer>(a.f0+ b.f0,a.f1+ b.f1);
                            }
                        });

        resStream.print();
        env.execute();
    }
}
