package com.zgg.api.tableapi.udf;

import com.zgg.api.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

public class AggrFunc {
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

        // 自定义聚合函数，求当前传感器的平均温度值
        AvgTemp avgTemp = new AvgTemp();
        // 注册自定义函数
        tableEnv.registerFunction("avgTemp", avgTemp);
        // table api
        Table resTable = table.groupBy("id")
                .aggregate("avgTemp(temp) as temp")
                .select("id, temp");

        // sql
        tableEnv.createTemporaryView("sensor",table);
        Table resSqlTable = tableEnv.sqlQuery("select id, avgTemp(temp) as temp from sensor group by id");

        tableEnv.toRetractStream(resTable, Row.class).print("result");
        tableEnv.toRetractStream(resSqlTable, Row.class).print("sql");

        env.execute();
    }

    // 实现自定义的AggregateFunction
    public static class AvgTemp extends AggregateFunction<Double, Tuple2<Double,Integer>>{

        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0,0);
        }

        // 来数据之后更新状态
        // 定义这个方法，必须是这个accumulate名称，第一个参数是当前状态，第二个参数是来的数据
        public void accumulate(Tuple2<Double,Integer> accumulator, Double temp){ // .aggregate("avgTemp(temp) as temp")
            accumulator.f0 += temp;
            accumulator.f1 += 1;
        }
    }
}
