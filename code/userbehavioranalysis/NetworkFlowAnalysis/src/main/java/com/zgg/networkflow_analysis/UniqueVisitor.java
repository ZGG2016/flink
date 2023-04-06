package com.zgg.networkflow_analysis;

import com.zgg.networkflow_analysis.beans.PageViewCount;
import com.zgg.networkflow_analysis.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/*
 * 实时流量统计 —— PV 和 【UV】
 *    – 从埋点日志中，统计实时的 PV 和 UV
 *    – 统计每小时的访问量（PV），并且对用户进行去重（UV）
 *
 *  解决思路
 *   – 统计埋点日志中的 pv 行为，利用 Set 数据结构进行去重
 * */
public class UniqueVisitor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

        // 读取文件，转换成POJO
        DataStream<String> inputStream = env.readTextFile("E:\\file\\正在进行中\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv");

        DataStream<UserBehavior> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(",");
                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {  // 数据源里的时间戳是升序的
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;  // 数据源里是秒
                    }
                });

        DataStream<PageViewCount> uvResStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))   // 过滤pv行为
                .timeWindowAll(Time.hours(1))  // 因为这里设的并行度为1，不需要并行计算，这里就可以让所有元素进入相同的算子实例
                .apply(new UvCountResult());

        uvResStream.print();
        env.execute("uv-count-job");
    }

    // 实现自定义全窗口函数
    public static class UvCountResult implements AllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {
        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<PageViewCount> out) throws Exception {
            // 定义一个Set结构，保存窗口中的所有userId，自动去重
            HashSet<Long> uidSet = new HashSet<>();
            for (UserBehavior ub:values){
                uidSet.add(ub.getUserId());
            }
            out.collect(new PageViewCount("uv", window.getEnd(), (long)uidSet.size()));
        }
    }
}
