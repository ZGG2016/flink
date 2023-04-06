package com.zgg.api.tableapi;

import com.zgg.api.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class GroupWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        String filePath = "src/main/resources/sensor.txt";
        DataStream<String> inStream = env.readTextFile(filePath);
        DataStream<SensorReading> dataStream = inStream.map(line -> {
            String[] valArr = line.split(",");
            return new SensorReading(valArr[0], Long.parseLong(valArr[1]), Double.parseDouble(valArr[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp() * 1000L;
            }
        });
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table table = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp, rt.rowtime");
        tableEnv.createTemporaryView("sensor", table);

        // table api
        Table resTable = table.window(Tumble.over("10.seconds").on("rt").as("tw"))
                              .groupBy("id, tw")
                              .select("id, id.count, temp.avg ,tw.start,tw.end");
        // sql
        Table resSqlTable = tableEnv.sqlQuery("select id,count(id) as cnt, avg(temp) as avgtemp, " +
                "tumble_start(rt, interval '10' second),tumble_end(rt, interval '10' second)" +
                "from sensor group by id, tumble(rt, interval '10' second)");

        tableEnv.toAppendStream(resTable, Row.class).print("table-api");
        tableEnv.toRetractStream(resSqlTable, Row.class).print("sql");

        env.execute();
    }
}
