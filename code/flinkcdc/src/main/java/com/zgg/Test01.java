package com.zgg;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启checkpoint
//        env.enableCheckpointing(5000);
//        env.getCheckpointConfig().setAlignmentTimeout(10000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//
//        env.setStateBackend(new FsStateBackend("hdfs://bigdata101:8020/cdc-test/ck"));

        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("bigdata101")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("cdc_test")  // 可以写多个库
                .tableList("cdc_test.user_info") // 可以写多个表；这里必须写库加表名；不写这个配置就是监控库下的所有表
//                .deserializer(new StringDebeziumDeserializationSchema())
                .deserializer(new CustomerDeserializationSchema())   // 使用自定义反序列化器
//                .startupOptions(StartupOptions.initial())  // 注意理解第一次读取（没有从checkpoint或savepoint恢复都是第一次）
                .startupOptions(StartupOptions.latest())  // 只加载最新的数据
                .build();

        DataStreamSource<String> inputStream = env.addSource(sourceFunction);
        inputStream.print();
        env.execute("flink cdc test");
    }
}
