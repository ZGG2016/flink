package com.zgg.api.window;

import com.zgg.api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Window {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<String> inDataStream = env.readTextFile("src/main/resources/sensor.txt");
        DataStream<SensorReading> dataStream = inDataStream.map(line -> {
            String[] valArr = line.split(",");
            return new SensorReading(valArr[0], Long.parseLong(valArr[1]), Double.parseDouble(valArr[2]));
        });

        DataStream<SensorReading> resStream = dataStream.keyBy("id")
                // window 的最底层的实现。 开会话窗口就得用 window 开
//                .window( EventTimeSessionWindows.withGap(Time.minutes(2)))
                // window的封装实现，开时间窗口，给一个参数就是滚动，两个参数就是滑动
//                .timeWindow(Time.seconds(10),3)
                // window的封装实现，开计数窗口，给一个参数就是滚动，两个参数就是滑动
                .countWindow(10, 3)
                .max("temperature");

        resStream.print();
        env.execute();
    }
}
