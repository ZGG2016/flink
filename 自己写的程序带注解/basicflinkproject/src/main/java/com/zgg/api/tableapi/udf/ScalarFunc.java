package com.zgg.api.tableapi.udf;

import com.zgg.api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

public class ScalarFunc {
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

        // 自定义标量函数，实现求id的hash值
        HashCode hashcode = new HashCode(23L);
        // 注册自定义函数
        tableEnv.registerFunction("hashCode",hashcode);
        // table api
        Table resTable = table.select("id, temp, hashCode(id)");

        // sql
        tableEnv.createTemporaryView("sensor",table);
        Table resSqlTable = tableEnv.sqlQuery("select id, temp, hashCode(id) from sensor");

        tableEnv.toAppendStream(resTable, Row.class).print("result");
        tableEnv.toAppendStream(resSqlTable, Row.class).print("sql");

        env.execute();
    }

    // 实现自定义的ScalarFunction
    public static class HashCode extends ScalarFunction{

        public Long factor = 13L;

        public HashCode(Long factor) {
            this.factor = factor;
        }
        // 定义这个方法，必须是这个方法名称，是 public
        public Long eval(String str){
            return str.hashCode() * factor;
        }
    }
}
