package com.zgg.api.transform;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapLambdaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inDataStream = env.readTextFile("src/main/resources/sensor.txt");

        /*
        * MapFunction 继承了 Function 接口，这个接口可以让实现类通过 Java 8 lambdas 实现
        * 而对于 wordcount 中实现的 flatMap 函数，虽然 FlatMapFunction 也继承了 Function 接口，但这种写法会报错
        * 如下：
        *  .....InvalidTypesException:
        *  The return type of function 'main(flatMapLambdaTest.java:20)' could not be determined automatically,
        *  due to type erasure. You can give type information hints by using the returns(...) method on the result
        *  of the transformation call, or by letting your function implement the 'ResultTypeQueryable' interface.
        *  以上就是说：由于泛型擦除，函数返回值不能自动推断出，可以在转换调用的结果处调用 returns 方法指定，或者让函数实现 ResultTypeQueryable 接口
        *
        *  ......InvalidTypesException: The generic type parameters of 'Collector' are missing. In many cases lambda methods
        *  don't provide enough information for automatic type extraction when Java generics are involved.
        *  An easy workaround is to use an (anonymous) class instead that implements the 'org.apache.flink.api.common.functions.
        *  FlatMapFunction' interface. Otherwise the type has to be specified explicitly using type information.
        *  以上就是说：缺少 Collector 的泛型类型。在涉及 java 泛型时，lambda 方法不能抽取出返回类型。
        *            一种解决方法就是使用匿名内部类的方式，另一种就是明确指定类型
         * */
        DataStream<String> flatMapDataStream = inDataStream.flatMap((String line, Collector<String> out) -> {
            String[] valArr = line.split(",");
            for (String word:valArr){
                out.collect(word);
            }
        });

        // 在转换调用的结果处调用 returns 方法指定
//        DataStream<String> flatMapDataStream = inDataStream.flatMap((String line, Collector<String> out) -> {
//            String[] valArr = line.split(",");
//            for (String val:valArr){
//                out.collect(val);
//            }
//        }).returns(String.class);

        // 明确指定类型
//        DataStream<String> flatMapDataStream = inDataStream.flatMap((String line, Collector<String> out) -> {
//            String[] valArr = line.split(",");
//            for (String word:valArr){
//                out.collect(word);
//            }
//        }, TypeInformation.of(String.class));

        flatMapDataStream.print();

        env.execute();
    }
}