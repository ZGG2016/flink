package com.atguigu.flink.tuning.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;

import java.text.SimpleDateFormat;

public class UvRichFilterFunction extends RichFilterFunction<JSONObject> {

    //声明状态
    private ValueState<String> firstVisitState;
    private SimpleDateFormat simpleDateFormat;

    @Override
    public void open(Configuration parameters) throws Exception {
        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("visit-state", String.class);

        //创建状态TTL配置项
        StateTtlConfig stateTtlConfig = StateTtlConfig
                .newBuilder(Time.days(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build();
        stringValueStateDescriptor.enableTimeToLive(stateTtlConfig);
        firstVisitState = getRuntimeContext().getState(stringValueStateDescriptor);
    }

    @Override
    public boolean filter(JSONObject value) throws Exception {

        //取出上一次访问页面
        String lastPageId = value.getJSONObject("page").getString("last_page_id");

        //判断是否存在上一个页面
        if (lastPageId == null || lastPageId.length() <= 0) {

            //取出状态数据
            String firstVisitDate = firstVisitState.value();

            //取出数据时间
            Long ts = value.getLong("ts");
            String curDate = simpleDateFormat.format(ts);

            if (firstVisitDate == null || !firstVisitDate.equals(curDate)) {
                firstVisitState.update(curDate);
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }

    }
}