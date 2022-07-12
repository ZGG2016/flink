package com.zgg.api.transform;

import com.zgg.api.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inDataStream = env.readTextFile("src/main/resources/sensor.txt");
        DataStream<SensorReading> mapDataStream = inDataStream.map(line -> {
            String[] valArr = line.split(",");
            return new SensorReading(valArr[0], Long.parseLong(valArr[1]), Double.parseDouble(valArr[2]));
        });
        KeyedStream<SensorReading, Tuple> keyedStream = mapDataStream.keyBy("id");

        /*
        * 使用 reduce 取出最大温度值，及当前时间
        * 注意观察输出，观察和 maxBy 输出结果的区别：
        * 1.结果是滚动输出，一直迭代输出
        * 2.在 sensor_1 分区，当第三条数据到来时，它的温度值没有当前最大的温度值大，所以温度值仍输出的是当前最大的温度值，
        *   但对应时间戳使用了当前第三条数据的温度值。而 maxBy 输出结果中，当前输出数据的温度值使用的仍是上条数据的温度值。
        * SensorReading{id='sensor_1', timestamp=1547718199, temperature=35.8}
        * SensorReading{id='sensor_1', timestamp=1547718207, temperature=36.3}
        * SensorReading{id='sensor_1', timestamp=1547718209, temperature=36.3}
        * SensorReading{id='sensor_1', timestamp=1547718212, temperature=37.1}
        * */
        DataStream<SensorReading> resStream = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                // value1: 之前聚合得到的结果  value2:当前传入的数据
                return new SensorReading(value1.getId(), value2.getTimestamp(),
                        Math.max(value1.getTemperature(), value2.getTemperature()));
            }
        });

//        DataStream<SensorReading> maxTemperatureAndLatestTime = keyedStream.reduce(
//                (curState,newState) -> {
//                    return new SensorReading(curState.getId(), newState.getTimestamp(),
//                            Math.max(curState.getTemperature(), newState.getTemperature()));
//                });

        resStream.print();
        env.execute();

    }
}
