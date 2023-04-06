package com.zgg.api.transform;

import com.zgg.api.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collection;
import java.util.Collections;

public class SplitConnectUnion {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inDataStream = env.readTextFile("src/main/resources/sensor.txt");
        DataStream<SensorReading> dataStream = inDataStream.map(line -> {
            String[] valArr = line.split(",");
            return new SensorReading(valArr[0], Long.parseLong(valArr[1]), Double.parseDouble(valArr[2]));
        });

        // 使用 split 划分流，但实际还是一个流，只是在这个流的内部划分了两个流，每个流都有一个标签，
        // 这样，流A中的数据输出到标签为A的输出中，流B中的数据输出到标签为B的输出中。
        SplitStream<SensorReading> splitStream = dataStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                return value.getTemperature() > 30 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });
        // 使用 select 选择前面添加的标签下的流
        DataStream<SensorReading> highStream = splitStream.select("high");
        DataStream<SensorReading> lowStream = splitStream.select("low");
        DataStream<SensorReading> allStream = splitStream.select("high","low");
//
//        highStream.print("high");
//        lowStream.print("low");
//        allStream.print("all");
        
        // 使用 connect 连接流，将高温流转换成二元组类型，与低温流连接合并之后，输出状态信息
//        DataStream<Tuple2<String, Double>> warningStream = highStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
//            @Override
//            public Tuple2<String, Double> map(SensorReading value) throws Exception {
//                return new Tuple2<>(value.getId(), value.getTemperature());
//            }
//        });
        // 和划分同理，连接后表面是一条流，但内部实际上还是两条流，
        // 此时，可以使用 CoMapFunction 分别操作这两条流
//        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = warningStream.connect(lowStream);
//        DataStream<Object> resStream = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
//
//            @Override
//            public Object map1(Tuple2<String, Double> value) throws Exception {
//                return new Tuple3<>(value.f0, value.f1, "warning");
//            }
//
//            @Override
//            public Object map2(SensorReading value) throws Exception {
//                return new Tuple3<>(value.getId(), value.getTemperature(), "ok");
//            }
//        });

        // union 可以合并多个类型相同的流，
        // 但 connect 只能合并两个，类型可以是不同的，但可以在之后的 coMap 中再去调整成为一样的
        DataStream<SensorReading> resStream = highStream.union(lowStream, allStream);
        resStream.print();
        env.execute();
    }
}
