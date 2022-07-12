package com.zgg.api.tableapi.udf;

import com.zgg.api.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class TableAggrFunc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<String> inStream = env.readTextFile("src/main/resources/sensor.txt");
        DataStream<SensorReading> dataStream = inStream.map(line -> {
            String[] valArr = line.split(",");
            return new SensorReading(valArr[0], Long.parseLong(valArr[1]), Double.parseDouble(valArr[2]));
        });
        Table table = tableEnv.fromDataStream(dataStream, "id, temperature as temp, timestamp as ts");

        // 自定义聚合函数，求当前传感器的top2温度值
        TopTemp topTemp = new TopTemp();
        // 注册自定义函数
        tableEnv.registerFunction("topTemp", topTemp);
        // table api
        Table resTable = table.groupBy("id")
                .flatAggregate("topTemp(temp) as (temp,rank)")
                .select("id, temp, rank");

        tableEnv.toRetractStream(resTable, Row.class).print("result");

        env.execute();
    }

    // 实现自定义的AggregateFunction
    // 输出：温度值和排名  中间状态：top1温度值、top2温度值
    public static class TopTemp extends TableAggregateFunction<Tuple2<Double,Integer>, Tuple2<Double,Double>> {

        @Override
        public Tuple2<Double, Double> createAccumulator() {
            return new Tuple2<>(0.0,0.0);
        }

        // 来数据之后更新状态
        // 定义这个方法，必须是这个accumulate名称，第一个参数是当前状态，第二个参数是来的数据
        public void accumulate(Tuple2<Double,Double> accumulator, Double temp){
            if (temp > accumulator.f0){
                accumulator.f1 = accumulator.f0;
                accumulator.f0 = temp;
            }else if (temp > accumulator.f1){
                accumulator.f1 = temp;
            }

        }

        // 定义这个方法，必须是这个emitValue名称
        public void emitValue(Tuple2<Double, Double> accumulator, Collector<Tuple2<Double, Integer>> out) {
            if (accumulator.f0 != 0.0){
                out.collect(new Tuple2<>(accumulator.f0,1));
            }
            if (accumulator.f1 != 0.0){
                out.collect(new Tuple2<>(accumulator.f1,2));
            }
        }
    }
}
