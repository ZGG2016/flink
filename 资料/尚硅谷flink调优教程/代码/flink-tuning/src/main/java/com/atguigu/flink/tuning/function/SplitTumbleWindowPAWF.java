package com.atguigu.flink.tuning.function;

import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;


public class SplitTumbleWindowPAWF extends ProcessAllWindowFunction<Long, Tuple3<String, String, Long>, TimeWindow> {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");

    @Override
    public void process(Context context, Iterable<Long> elements, Collector<Tuple3<String, String, Long>> out) throws Exception {
        Long uvCount = elements.iterator().next();
        String windowstartDate = sdf.format(context.window().getStart());
        String windowEndDate = sdf.format(context.window().getEnd());
        out.collect(Tuple3.of(windowstartDate, windowEndDate, uvCount));
    }
}
