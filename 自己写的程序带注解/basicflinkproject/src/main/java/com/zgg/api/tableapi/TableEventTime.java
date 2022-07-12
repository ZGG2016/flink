package com.zgg.api.tableapi;

import com.zgg.api.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class TableEventTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        String filePath = "src/main/resources/sensor.txt";
//        DataStream<String> inStream = env.readTextFile(filePath);
//        DataStream<SensorReading> dataStream = inStream.map(line -> {
//            String[] valArr = line.split(",");
//            return new SensorReading(valArr[0], Long.parseLong(valArr[1]), Double.parseDouble(valArr[2]));
//        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(1)) {
//            @Override
//            public long extractTimestamp(SensorReading element) {
//                return element.getTimestamp() * 1000L;
//            }
//        });
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        // 定义事件时间
//        // 1.方式一
////        Table sensor = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature, rt.rowtime");
//        // 这种方式会修改timestamp原始数据： sensor_1,2019-01-17 09:43:29.0,32.8
//        Table sensor = tableEnv.fromDataStream(dataStream, "id, timestamp.rowtime as ts, temperature");
//        sensor.printSchema();
//        tableEnv.toAppendStream(sensor, Row.class).print();

//---------------------------------------------------------------------------------------------------------------
//        // 2.方式二
//        // 定义 Table Schema 时指定
//        // 这个flink版本会有错误  ???
//        tableEnv.connect(new FileSystem().path(filePath))
//                        .withFormat( new Csv())
//                        .withSchema( new Schema()
//                                .field("id", DataTypes.STRING())
//                                .field("ts", DataTypes.BIGINT())
//                                .field("temperature", DataTypes.DOUBLE())
//                                .rowtime(
//                                        new Rowtime().timestampsFromField("ts").watermarksPeriodicBounded(1000)
//                                ))
//                        .createTemporaryTable("sensor");
//
//        Table sensor = tableEnv.from("sensor");
//        tableEnv.toAppendStream(sensor, Row.class).print();

//---------------------------------------------------------------------------------------------------------------
        // 基于Blink的流处理
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);

        // 3.方式三  要使用BLINK版本
        // 在创建表的 DDL 中定义
        String sqlDDL =
                "create table sensor (" +
                        " id varchar(20) not null, " +
                        " ts bigint, " +
                        " temperature double, " +
                        " rt AS to_timestamp(from_unixtime(ts))," +
                        " watermark for rt as rt - interval '1' second " +
                        ") with (" +
                        " 'connector.type' = 'filesystem', " +
                        " 'connector.path' = 'src/main/resources/sensor.txt', " +
                        " 'format.type' = 'csv')";

        blinkStreamTableEnv.sqlUpdate(sqlDDL);
        String sql = "select id, ts, temperature, rt from sensor where id = 'sensor_6'";
        Table table = blinkStreamTableEnv.sqlQuery(sql);
        blinkStreamTableEnv.toAppendStream(table, Row.class).print();
        env.execute();
    }
}
