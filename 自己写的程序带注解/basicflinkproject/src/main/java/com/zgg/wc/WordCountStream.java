package com.zgg.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountStream {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
//        senv.disableOperatorChaining();

        // 创建流处理执行环境
//        String inpath = "src/main/resources/hello.txt";
//        DataStream<String> inDataStream = senv.readTextFile(inpath);

        // 用parameter tool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // 从socket文本流读取数据
        DataStream<String> inDataStream = env.socketTextStream(host, port);
        
        DataStream<Tuple2<String, Integer>> resDataStream = inDataStream.flatMap(new WordCount.MyFlatMapper()).slotSharingGroup("green")
                .keyBy(0)
                .sum(1).slotSharingGroup("red");
//                .disableChaining();
//                .startNewChain();

        resDataStream.print();

        env.execute();
    }
}
