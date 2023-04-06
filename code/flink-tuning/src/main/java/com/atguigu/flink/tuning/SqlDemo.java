package com.atguigu.flink.tuning;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.flink.source.MockSourceFunction;
import com.atguigu.flink.tuning.bean.AppCommonWithDay;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;


public class SqlDemo {
    public static void main(String[] args) throws Exception {

//        Configuration conf = new Configuration();
//        conf.set(RestOptions.ENABLE_FLAMEGRAPH, true);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setParallelism(1);
        env.disableOperatorChaining();


        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(3), CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://hadoop1:8020/flink-tuning/ck");
//        checkpointConfig.setCheckpointStorage("file:///F:/flink-tuning/test/ck");
        checkpointConfig.setMinPauseBetweenCheckpoints(TimeUnit.SECONDS.toMillis(3));
        checkpointConfig.setTolerableCheckpointFailureNumber(5);
        checkpointConfig.setCheckpointTimeout(TimeUnit.MINUTES.toMillis(1));
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        SingleOutputStreamOperator<AppCommonWithDay> commonDayDS = env
                .addSource(new MockSourceFunction())
                .map(new MapFunction<String, AppCommonWithDay>() {
                         SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

                         @Override
                         public AppCommonWithDay map(String value) throws Exception {
                             JSONObject jsonObject = JSONObject.parseObject(value);
                             JSONObject commonObj = jsonObject.getJSONObject("common");
                             Long ts = jsonObject.getLong("ts");
                             String day = sdf.format(ts);
                             commonObj.put("day", day);
                             AppCommonWithDay appCommonWithDay = JSON.parseObject(commonObj.toJSONString(), AppCommonWithDay.class);
                             return appCommonWithDay;
                         }
                     }
                );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofDays(1));

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        boolean isMiniBatch = parameterTool.getBoolean("minibatch", false);
        boolean isLocalGlobal = parameterTool.getBoolean("local-global", false);
        boolean isSplitDistinct = parameterTool.getBoolean("split-distinct", false);

        Configuration configuration = tableEnv.getConfig().getConfiguration();
        if (isMiniBatch){
            // 开启miniBatch
            configuration.setString("table.exec.mini-batch.enabled", "true");
            // 批量输出的间隔时间
            configuration.setString("table.exec.mini-batch.allow-latency", "5 s");
            // 防止OOM设置每个批次最多缓存数据的条数，可以设为2万条
            configuration.setString("table.exec.mini-batch.size", "20000");
        }
        if (isLocalGlobal){
            // 开启LocalGlobal
            configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
        }
        if (isSplitDistinct){
            // 开启Split Distinct
            configuration.setString("table.optimizer.distinct-agg.split.enabled", "true");
            // 第一层打散的bucket数目,默认1024
            configuration.setString("table.optimizer.distinct-agg.split.bucket-num", "1024");
        }

        String demo = parameterTool.get("demo", "uv");
        String execSql = "";
        String fieldsStr = "";
        switch (demo) {
            case "dim-difcount":{
                execSql="SELECT\n" +
                        "   mid,\n" +
                        "   COUNT(DISTINCT ar) as ar_difcount,\n" +
                        "   COUNT(DISTINCT CASE WHEN ch IN ('web') THEN ar ELSE NULL END) as web_ar_difcount,\n" +
                        "   COUNT(DISTINCT CASE WHEN ch IN ('wandoujia') THEN ar ELSE NULL END) as wdj_ar_difcount\n" +
                        "FROM common_table\n" +
                        "GROUP BY mid";
                fieldsStr = "mid String,ar_difcount BIGINT,web_ar_difcount BIGINT,wdj_ar_difcount BIGINT";
                break;
            }
            case "dim-difcount-filter":{
                execSql="SELECT\n" +
                        "   mid,\n" +
                        "   COUNT(DISTINCT ar) as ar_difcount,\n" +
                        "   COUNT(DISTINCT ar) FILTER (WHERE ch IN ('web')) as web_ar_difcount,\n" +
                        "   COUNT(DISTINCT ar) FILTER (WHERE ch IN ('wandoujia')) as wdj_ar_difcount\n" +
                        "FROM common_table\n" +
                        "GROUP BY mid";
                fieldsStr = "mid String,ar_difcount BIGINT,web_ar_difcount BIGINT,wdj_ar_difcount BIGINT";
                break;
            }
            case "count": {
                execSql = "SELECT \n" +
                        "    `day`,\n" +
                        "    mid, \n" +
                        "    COUNT(1) as mid_count\n" +
                        "FROM common_table\n" +
                        "GROUP BY `day`,mid";
                fieldsStr = "`day` String,mid STRING,mid_count BIGINT";
                break;
            }
            case "distinct":
            default: {
                execSql = "SELECT \n" +
                        "   mid, \n" +
                        "   COUNT(DISTINCT ar) as ar_discount\n" +
                        "FROM common_table\n" +
                        "GROUP BY mid";
                fieldsStr = "mid String,ar_difcount BIGINT";
                break;
            }

        }


        tableEnv.createTemporaryView("common_table", commonDayDS);

        String printSql = "create table `print_table`(\n" +
                fieldsStr + "\n" +
                ")with(\n" +
                "    'connector' = 'print'\n" +
                ")";
        tableEnv.executeSql(printSql);
        tableEnv.executeSql("insert into print_table " + execSql);

    }
}

/*
{
        "common": {
            "ar": "230000",
            "ba": "iPhone",
            "ch": "Appstore",
            "is_new": "0",
            "md": "iPhone X",
            "mid": "mid_197",
            "os": "iOS 13.3.1",
            "uid": "373",
            "vc": "v2.1.132"
        },
        "start": {
            "entry": "install",
            "loading_time": 7927,
            "open_ad_id": 4,
            "open_ad_ms": 7068,
            "open_ad_skip_ms": 0
        },
        "ts": 1636627931000
    },
    {
        "common": {
            "ar": "230000",
            "ba": "iPhone",
            "ch": "Appstore",
            "is_new": "0",
            "md": "iPhone X",
            "mid": "mid_197",
            "os": "iOS 13.3.1",
            "uid": "373",
            "vc": "v2.1.132"
        },
        "displays": [
            {
                "display_type": "activity",
                "item": "1",
                "item_type": "activity_id",
                "order": 1,
                "pos_id": 3
            },
            {
                "display_type": "promotion",
                "item": "4",
                "item_type": "sku_id",
                "order": 2,
                "pos_id": 5
            },
            {
                "display_type": "query",
                "item": "3",
                "item_type": "sku_id",
                "order": 3,
                "pos_id": 3
            },
            {
                "display_type": "promotion",
                "item": "6",
                "item_type": "sku_id",
                "order": 4,
                "pos_id": 5
            },
            {
                "display_type": "query",
                "item": "10",
                "item_type": "sku_id",
                "order": 5,
                "pos_id": 2
            },
            {
                "display_type": "query",
                "item": "6",
                "item_type": "sku_id",
                "order": 6,
                "pos_id": 2
            }
        ],
        "page": {
            "during_time": 13442,
            "page_id": "home"
        },
        "ts": 1636627931000
    }
 */