package com.zgg.api.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadTextFile {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> dataStream = env.readTextFile("src/main/resources/sensor.txt");

        // 打印输出
        dataStream.print();

        env.execute();
    }
}
