package com.zgg.api.window;

import com.zgg.api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class Late {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inStream = env.socketTextStream("bigdata101",7777);
        DataStream<SensorReading> dataStream = inStream.map(line -> {
            String[] valArr = line.split(",");
            return new SensorReading(valArr[0], Long.parseLong(valArr[1]), Double.parseDouble(valArr[2]));
        });

        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late");
        SingleOutputStreamOperator<SensorReading> resStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .max("temperature");

        DataStream<SensorReading> sideOutputStream = resStream.getSideOutput(outputTag);

        resStream.print("normal");
        sideOutputStream.print("late");
        env.execute();
    }
}
