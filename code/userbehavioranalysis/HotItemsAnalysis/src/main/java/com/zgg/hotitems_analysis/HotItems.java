package com.zgg.hotitems_analysis;

import com.zgg.hotitems_analysis.beans.ItemViewCount;
import com.zgg.hotitems_analysis.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Properties;

public class HotItems {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 读取数据，创建DataStream
        // 2.1 从文件读取
//        String filePath = "E:\\file\\正在进行中\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv";
//        DataStream<String> inputStream = env.readTextFile(filePath);
        // 2.2 从kafka读取
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "bigdata101:9092");
        properties.setProperty("group.id", "consumer");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>("hotitems", new SimpleStringSchema(), properties));


        // 3. 转换为POJO，分配时间戳和watermark
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

        // 4. 分组开窗聚合，得到每个窗口内各个商品的count值
        DataStream<ItemViewCount> windowAggStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))   // 过滤pv行为
                .keyBy("itemId")  // 按商品ID分组
                .timeWindow(Time.hours(1), Time.minutes(5))  // 开滑窗
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());  // 先增量聚合，计算出每个商品的计数，再利用全窗口聚合，取到窗口信息，即windowEnd，再输出

        // 5. 收集同一窗口的所有商品count数据，排序输出top n
        DataStream<String> resStream = windowAggStream
                .keyBy("windowEnd")  // 按照窗口分组
                .process(new TopNHotItems(5));  // 用自定义处理函数排序取前5

        resStream.print();
        env.execute("hot-items-topn");
    }

    // 实现自定义增量聚合函数，计算出每个商品的计数
    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    // 自定义全窗口函数，取到窗口信息，即windowEnd，再输出
    // 因为每条数据可能属于多个窗口，取出窗口信息区分不同窗口，方便后面 ，按窗口分组，在窗口内排名
    public static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            Long itemId = tuple.getField(0);
            Long windowEnd = window.getEnd();
            Long count = input.iterator().next();
            out.collect( new ItemViewCount(itemId, windowEnd, count));
        }
    }

    // 实现自定义KeyedProcessFunction
    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

        public Integer topSize;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        // 定义列表状态，保存当前窗口内所有输出的ItemViewCount
        public ListState<ItemViewCount> itemViewCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountState = getRuntimeContext().getListState( new ListStateDescriptor<ItemViewCount>("item-view-count-state", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, KeyedProcessFunction<Tuple, ItemViewCount, String>.Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据，存入List中，并注册定时器
            itemViewCountState.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Tuple, ItemViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，当前已收集到所有数据，排序输出
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountState.get().iterator());
            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.getCount().intValue() - o1.getCount().intValue();
                }
            });

            // 将排名信息格式化成String，方便打印输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("===================================\n");
            resultBuilder.append("窗口结束时间：").append( new Timestamp(timestamp - 1)).append("\n");

            // 遍历列表，取top n输出
            for( int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++ ){
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                resultBuilder.append("NO ").append(i+1).append(":")
                        .append(" 商品ID = ").append(currentItemViewCount.getItemId())
                        .append(" 热门度 = ").append(currentItemViewCount.getCount())
                        .append("\n");
            }
            resultBuilder.append("===============================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());

        }

        @Override
        public void close() throws Exception {
            itemViewCountState.clear();
        }
    }
}
