package com.zgg.api.transform;

import com.zgg.api.beans.SensorReading;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RRichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<String> inDataStream = env.readTextFile("src/main/resources/sensor.txt");
        DataStream<SensorReading> dataStream = inDataStream.map(line -> {
            String[] valArr = line.split(",");
            return new SensorReading(valArr[0], Long.parseLong(valArr[1]), Double.parseDouble(valArr[2]));
        });

        DataStream<Tuple2<String, Integer>> resStream = dataStream.map(new MyRichMapper());
        resStream.print();
        env.execute();
    }

    public static class MyRichMapper extends RichMapFunction<SensorReading, Tuple2<String,Integer>> {

        @Override
        public void open(Configuration parameters) throws Exception {
            // 以下可以做一些初始化工作，例如建立一个和 HDFS 的连接
            System.out.println("open");
        }

        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            return new Tuple2<>(value.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void close() throws Exception {
            // 以下做一些清理工作，例如断开和 HDFS 的连接
            System.out.println("close");
        }
    }
}
