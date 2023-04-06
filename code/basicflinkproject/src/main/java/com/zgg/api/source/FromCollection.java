package com.zgg.api.source;

import com.zgg.api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class FromCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从集合中读取数据
        DataStream<SensorReading> collectionDataStream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        ));

        // 根据一个个元素创建
        DataStream<Integer> integerDataStream = env.fromElements(1, 2, 3);

        collectionDataStream.print("collection");
        integerDataStream.print("integer");

        env.execute();

    }
}
