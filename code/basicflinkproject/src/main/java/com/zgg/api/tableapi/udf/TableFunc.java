package com.zgg.api.tableapi.udf;

import com.zgg.api.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class TableFunc {
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

        // 自定义表函数，实现将id拆分，并输出（word, length）
        Split split = new Split("_");
        // 注册自定义函数
        tableEnv.registerFunction("split",split);
        // table api
        Table resTable = table.joinLateral("split(id) as (word, length)")
                .select("id, temp, word, length");

        // sql
        tableEnv.createTemporaryView("sensor",table);
        Table resSqlTable = tableEnv.sqlQuery("select id, temp, word, length " +
                "from sensor, lateral table(split(id)) as splitid(word, length)");

        tableEnv.toAppendStream(resTable, Row.class).print("result");
        tableEnv.toAppendStream(resSqlTable, Row.class).print("sql");

        env.execute();
    }

    // 实现自定义的ScalarFunction
    public static class Split extends TableFunction<Tuple2<String,Integer>>{

        public String separator = ",";

        public Split(String separator) {
            this.separator = separator;
        }
        // 定义这个方法，必须是这个eval名称，没有返回值，是 public
        public void eval(String str){
            String[] strArr = str.split(separator);
            for (String s: strArr){
                collect(new Tuple2<>(s,s.length()));
            }
        }
    }
}
