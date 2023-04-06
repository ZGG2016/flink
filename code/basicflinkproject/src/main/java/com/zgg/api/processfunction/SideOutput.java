package com.zgg.api.processfunction;

import com.zgg.api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutput {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inStream = env.socketTextStream("bigdata101",7777);
        DataStream<SensorReading> dataStream = inStream.map(line -> {
            String[] valArr = line.split(",");
            return new SensorReading(valArr[0], Long.parseLong(valArr[1]), Double.parseDouble(valArr[2]));
        });

        // 定义一个OutputTag，用来表示侧输出流低温流
        // 注意这里的花括号，如果不加会报错：
        // Exception in thread "main" org.apache.flink.api.common.functions.InvalidTypesException:
        // Could not determine TypeInformation for the OutputTag type. The most common reason is
        // forgetting to make the OutputTag an anonymous inner class. It is also not possible to
        // use generic type variables with OutputTags, such as 'Tuple2<A, B>'.
        //
        // Caused by: org.apache.flink.api.common.functions.InvalidTypesException: The types of
        // the interface org.apache.flink.util.OutputTag could not be inferred. Support for synthetic
        // interfaces, lambdas, and generic or raw types is limited at this point
        //
        // 这个 OutputTag 类是有泛型的，但没有在构造方法里使用，而是它在构造方法里使用反射抽取这个类型，所以会有泛型擦除
        // 所以这里通过匿名内部类实现
        OutputTag<SensorReading> lowTempTag = new OutputTag<SensorReading>("low-temp"){};

        // 测试ProcessFunction，自定义侧输出流实现分流操作
        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, ProcessFunction<SensorReading, SensorReading>.Context ctx, Collector<SensorReading> out) throws Exception {
                // 判断温度，大于30度，高温流输出到主流；小于低温流输出到侧输出流
                if(value.getTemperature() > 30){
                    out.collect( value );
                }else{
                    ctx.output( lowTempTag, value );
                }
            }
        });

        highTempStream.print("high");
        highTempStream.getSideOutput(lowTempTag).print("low");

        env.execute();
    }
}
