package com.zgg.api.tableapi;

import com.zgg.api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inStream = env.readTextFile("src/main/resources/sensor.txt");
        DataStream<SensorReading> dataStream = inStream.map(line -> {
            String[] valArr = line.split(",");
            return new SensorReading(valArr[0], Long.parseLong(valArr[1]), Double.parseDouble(valArr[2]));
        });

        // 创建表环境，table api sql 的入口
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. TABLE API
        // 把dataStream转换成table类型
        Table table = tableEnv.fromDataStream(dataStream);
        Table resTable = table.select("id, temperature")
                .where("id = 'sensor_1'");

        // 2. FLINK SQL
        tableEnv.createTemporaryView("sensor",dataStream);
        String sql = "select id, temperature from sensor where id = 'sensor_1' ";
        Table resSqlTable = tableEnv.sqlQuery(sql);

        // 把table类型转换成dataStream
        tableEnv.toAppendStream(resTable, Row.class).print("table");
        tableEnv.toAppendStream(resSqlTable, Row.class).print("sql-table");

        env.execute();
    }
}
