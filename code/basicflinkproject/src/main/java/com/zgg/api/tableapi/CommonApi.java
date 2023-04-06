package com.zgg.api.tableapi;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public class CommonApi {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        // 1.1 基于老版本planner的流处理
//        EnvironmentSettings oldStreamSettings = EnvironmentSettings.newInstance()
//                .useOldPlanner()
//                .inStreamingMode()
//                .build();
//        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, oldStreamSettings);
//
//        // 1.2 基于老版本planner的批处理
//        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);
//
//        // 1.3 基于Blink的流处理
//        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
//                .inStreamingMode()
//                .build();
//        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);
//
//        // 1.4 基于Blink的批处理
//        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
//                .inBatchMode()
//                .build();
//        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings);


        // 2. 表的创建：连接外部系统，读取数据
        // 2.1 读取文件
        String inPath = "src/main/resources/sensor.txt";
        // 注册一个输入表
        tableEnv.connect( new FileSystem().path(inPath))
                .withFormat( new Csv())
                .withSchema( new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("inTable");

        // 开始读数据
        Table inTable = tableEnv.from("inTable");
//        inTable.printSchema();
//        tableEnv.toAppendStream(inTable, Row.class).print();

        // 3. 查询转换
        // 3.1 Table API
        // 简单转换
        Table queryTable1 = inTable.select("id, temp").filter("id = 'sensor_6'");
        // 聚合统计
        Table aggTable1 = inTable.groupBy("id")
                .select("id, id.count as count, temp.avg as temp");

        // 3.2 SQL
        // 简单转换
        String sql1 = "select id, temp from inTable where id = 'sensor_6'";
        Table queryTable2 = tableEnv.sqlQuery(sql1);
        // 聚合统计
        String sql2 = "select id, count(id) as scount, avg(temp) as stemp from inTable group by id";
        Table aggTable2 = tableEnv.sqlQuery(sql2);

//        tableEnv.toAppendStream(queryTable1,Row.class).print("queryTable1");
//        tableEnv.toRetractStream(aggTable1,Row.class).print("aggTable1");
//        tableEnv.toRetractStream(queryTable2,Row.class).print("queryTable2");
//        tableEnv.toRetractStream(aggTable2,Row.class).print("aggTable2");

        // 4. 输出到文件
        // 注册一个输出表
        String outPath = "src/main/resources/out.txt";
        tableEnv.connect( new FileSystem().path(outPath))
                .withFormat( new Csv())
                .withSchema( new Schema()
                        .field("id", DataTypes.STRING())
                        .field("cnt", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("outTable");

//        queryTable1.insertInto("outTable");
        // 报错: ...api.TableException: AppendStreamTableSink requires that Table has only insert changes.
        aggTable1.insertInto("outTable");

        env.execute();
    }
}
