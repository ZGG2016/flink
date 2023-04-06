package com.zgg.networkflow_analysis;

import com.zgg.networkflow_analysis.beans.PageViewCount;
import com.zgg.networkflow_analysis.beans.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.HashSet;

/*
 * 实时流量统计 —— PV 和 【UV】【使用布隆过滤器】
 *    – 从埋点日志中，统计实时的 PV 和 UV
 *    – 统计每小时的访问量（PV），并且对用户进行去重（UV）
 *
 *  解决思路
 *   – 对于超大规模的数据，可以考虑用布隆过滤器进行去重
 * */
public class UniqueVisitorWithBloomFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

        // 读取文件，转换成POJO
        String filePath = "E:\\file\\正在进行中\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv";
        DataStream<String> inputStream = env.readTextFile(filePath);
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
                .trigger( new MyTrigger() )  // 来一条数据就触发一次计算提交
                .process( new UvCountResultWithBloomFilter() );

        uvResStream.print();
        env.execute("uv-count-job");
    }
    public static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {

        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            // 每一条数据来到，直接触发窗口计算，并且直接清空窗口
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        }
    }

    public static class UvCountResultWithBloomFilter extends ProcessAllWindowFunction<UserBehavior, PageViewCount,TimeWindow>{
        Jedis jedis;
        MyBloomFilter myBloomFilter;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("localhost", 6379);
            myBloomFilter = new MyBloomFilter(1<<29);    // 要处理1亿个数据，用64MB大小的位图
        }

        @Override
        public void process(ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow>.Context context, Iterable<UserBehavior> elements, Collector<PageViewCount> out) throws Exception {
            // 将位图和窗口count值全部存入redis，用windowEnd作为key
            Long windowEnd = context.window().getEnd();
            String bitmapKey = windowEnd.toString();
            // 把count值存成一张hash表
            String countHashName = "uv_count";
            String countKey = windowEnd.toString();

            // 1. 取当前的userId
            Long userId = elements.iterator().next().getUserId();

            // 2. 计算位图中的offset
            Long offset = myBloomFilter.hashCode(userId.toString(), 61);

            // 3. 用redis的getbit命令，判断对应位置的值
            Boolean isExist = jedis.getbit(bitmapKey, offset);

            if( !isExist ){
                // 如果不存在，对应位图位置置1
                jedis.setbit(bitmapKey, offset, true);

                // 更新redis中保存的count值
                Long uvCount = 0L;    // 初始count值
                String uvCountString = jedis.hget(countHashName, countKey);
                if( uvCountString != null && !"".equals(uvCountString) )
                    uvCount = Long.valueOf(uvCountString);
                jedis.hset(countHashName, countKey, String.valueOf(uvCount + 1));

                out.collect(new PageViewCount("uv", windowEnd, uvCount + 1));
            }

        }

        @Override
        public void close() throws Exception {
            jedis.close();
        }
    }

    // 自定义一个布隆过滤器
    public static class MyBloomFilter {
        // 定义位图的大小，一般需要定义为2的整次幂
        private Integer cap;

        public MyBloomFilter(Integer cap) {
            this.cap = cap;
        }
        // 实现一个hash函数
        public Long hashCode( String value, Integer seed ){
            Long result = 0L;
            for( int i = 0; i < value.length(); i++ ){
                result = result * seed + value.charAt(i);
            }
            return result & (cap - 1);
        }
    }

}
