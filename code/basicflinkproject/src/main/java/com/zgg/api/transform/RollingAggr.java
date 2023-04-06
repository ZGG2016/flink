package com.zgg.api.transform;

import com.zgg.api.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class RollingAggr {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inDataStream = env.readTextFile("src/main/resources/sensor.txt");

        // 将每条记录封装成 SensorReading 对象
//        DataStream<SensorReading> mapDataStream = inDataStream.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String value) throws Exception {
//                String[] valArr = value.split(",");
//                return new SensorReading(valArr[0], Long.parseLong(valArr[1]), Double.parseDouble(valArr[2]));
//            }
//        });
        // MapFunction 继承了 Function 接口，这个接口可以让实现类通过 Java 8 lambdas 实现，但 flatMap 不行，具体见 flatMapLambdaTest.java
        DataStream<SensorReading> mapDataStream = inDataStream.map( line -> {
            String[] valArr = line.split(",");
            return new SensorReading(valArr[0], Long.parseLong(valArr[1]), Double.parseDouble(valArr[2]));
        });

        // 直接传用作 key 的字段名字，但不能使用字段位置，因为输入类型是一个对象
        // 不管是名字还是位置，返回类型固定是元组 Tuple
//        KeyedStream<SensorReading, Tuple> keyedStream = mapDataStream.keyBy("id");
        // 还可以传入 KeySelector 的实现类，返回类型不再固定是元组，而是 key 的类型，即 id 的类型字符串
//        KeyedStream<SensorReading, String> keyedStream = mapDataStream.keyBy(new KeySelector<SensorReading, String>() {
//            @Override
//            public String getKey(SensorReading value) throws Exception {
//                return value.getId();
//            }
//        });
        // 也可以使用 lambda
//        KeyedStream<SensorReading, String> keyedStream = mapDataStream.keyBy(line -> line.getId());
        // 也可以使用方法引用
        KeyedStream<SensorReading, String> keyedStream = mapDataStream.keyBy(SensorReading::getId);

        /*
        * 使用 max 取最大温度值
        * 注意观察输出：
        * 1.结果是滚动输出，一直迭代
        * 2.输出的 SensorReading 对象，只有 temperature 字段发生了变化，前两个字段一直是第一次输出的内容
        * SensorReading{id='sensor_1', timestamp=1547718199, temperature=35.8}
        * SensorReading{id='sensor_1', timestamp=1547718199, temperature=36.3}
        * SensorReading{id='sensor_1', timestamp=1547718199, temperature=36.3}
        * SensorReading{id='sensor_1', timestamp=1547718199, temperature=37.1}
        * */
//        DataStream<SensorReading> resStream = keyedStream.max("temperature");
        /*
         * 使用 maxBy 取最大温度值
         * 注意观察输出，观察和 max 输出结果的区别：
         * 1.结果是滚动输出，一直迭代输出
         * 2.输出的 SensorReading 对象，三个字段都发生了变化，即前两个字段和第三个字段是匹配的
         * 【这只取了一个分区的数据】
         * SensorReading{id='sensor_1', timestamp=1547718199, temperature=35.8}
         * SensorReading{id='sensor_1', timestamp=1547718207, temperature=36.3}
         * SensorReading{id='sensor_1', timestamp=1547718207, temperature=36.3}
         * SensorReading{id='sensor_1', timestamp=1547718212, temperature=37.1}
         * */
//        DataStream<SensorReading> resStream = keyedStream.maxBy("temperature");
        /*
         * 使用 sum 取温度和
         * 注意观察输出：
         * 1.结果是滚动输出，一直迭代
         * 2.输出的 SensorReading 对象，只有 temperature 字段发生了变化，前两个字段一直是第一次输出的内容
         * 【这只取了一个分区的数据】
         * SensorReading{id='sensor_1', timestamp=1547718199, temperature=35.8}
         * SensorReading{id='sensor_1', timestamp=1547718199, temperature=72.1}
         * SensorReading{id='sensor_1', timestamp=1547718199, temperature=104.89999999999999}
         * SensorReading{id='sensor_1', timestamp=1547718199, temperature=142.0}
         * */
        DataStream<SensorReading> resStream = keyedStream.sum("temperature");

        resStream.print();
        env.execute();
    }
}
