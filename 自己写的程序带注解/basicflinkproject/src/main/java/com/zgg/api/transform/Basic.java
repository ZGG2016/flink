package com.zgg.api.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Basic {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inDataStream = env.readTextFile("src/main/resources/sensor.txt");

        // 1 map  输出每条数据的长度
        DataStream<Integer> mapDataStream = inDataStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return value.length();
            }
        });

        // 2 flatmap 打散数据，依次输出
        DataStream<String> flatMapDataStream = inDataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] valArr = value.split(",");
                for (String val : valArr) {
                    out.collect(val);
                }
            }
        });

        // 3. filter 过滤出温度值大于20的数据
        DataStream<String> filterDataStream = inDataStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                String[] valArr = value.split(",");
                return Double.parseDouble(valArr[2]) > 20.0;
            }
        });


        mapDataStream.print("map");
        flatMapDataStream.print("flatMap");
        filterDataStream.print("filter");

        env.execute();
    }
}
