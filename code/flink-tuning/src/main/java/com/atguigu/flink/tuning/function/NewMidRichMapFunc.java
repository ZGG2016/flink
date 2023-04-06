package com.atguigu.flink.tuning.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

import java.text.SimpleDateFormat;

public class NewMidRichMapFunc extends RichMapFunction<JSONObject, JSONObject> {

        //声明状态用于表示当前Mid是否已经访问过
        private ValueState<String> firstVisitDateState;
        private SimpleDateFormat simpleDateFormat;

        @Override
        public void open(Configuration parameters) throws Exception {
            firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("new-mid", String.class));
            simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        }

        @Override
        public JSONObject map(JSONObject value) throws Exception {

            //取出新用户标记
            String isNew = value.getJSONObject("common").getString("is_new");

            //如果当前前端传输数据表示为新用户,则进行校验
            if ("1".equals(isNew)) {

                //取出状态数据并取出当前访问时间
                String firstDate = firstVisitDateState.value();
                Long ts = value.getLong("ts");

                //判断状态数据是否为Null
                if (firstDate != null) {
                    //修复
                    value.getJSONObject("common").put("is_new", "0");
                } else {
                    //更新状态
                    firstVisitDateState.update(simpleDateFormat.format(ts));
                }
            }

            //返回数据
            return value;
        }
    }