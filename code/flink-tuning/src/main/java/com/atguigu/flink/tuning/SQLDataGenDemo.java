package com.atguigu.flink.tuning;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class SQLDataGenDemo {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set(RestOptions.ENABLE_FLAMEGRAPH, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.setParallelism(1);
        env.disableOperatorChaining();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String orderSql="CREATE TABLE order_info (\n" +
                "    id INT,\n" +
                "    user_id BIGINT,\n" +
                "    total_amount DOUBLE,\n" +
                "    create_time AS localtimestamp,\n" +
                "    WATERMARK FOR create_time AS create_time\n" +
                ") WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'rows-per-second'='20000',\n" +
                "    'fields.id.kind'='sequence',\n" +
                "    'fields.id.start'='1',\n" +
                "    'fields.id.end'='100000000',\n" +
                "    'fields.user_id.kind'='random',\n" +
                "    'fields.user_id.min'='1',\n" +
                "    'fields.user_id.max'='1000000',\n" +
                "    'fields.total_amount.kind'='random',\n" +
                "    'fields.total_amount.min'='1',\n" +
                "    'fields.total_amount.max'='1000'\n" +
                ")";

        String userSql="CREATE TABLE user_info (\n" +
                "    id INT,\n" +
                "    user_id BIGINT,\n" +
                "    age INT,\n" +
                "    sex INT\n" +
                ") WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'rows-per-second'='20000',\n" +
                "    'fields.id.kind'='sequence',\n" +
                "    'fields.id.start'='1',\n" +
                "    'fields.id.end'='100000000',\n" +
                "    'fields.user_id.kind'='sequence',\n" +
                "    'fields.user_id.start'='1',\n" +
                "    'fields.user_id.end'='1000000',\n" +
                "    'fields.age.kind'='random',\n" +
                "    'fields.age.min'='1',\n" +
                "    'fields.age.max'='100',\n" +
                "    'fields.sex.kind'='random',\n" +
                "    'fields.sex.min'='0',\n" +
                "    'fields.sex.max'='1'\n" +
                ")";


        tableEnv.executeSql(orderSql);
        tableEnv.executeSql(userSql);

        tableEnv.executeSql("select * from order_info").print();
//        tableEnv.executeSql("select * from user_info").print();

    }
}
