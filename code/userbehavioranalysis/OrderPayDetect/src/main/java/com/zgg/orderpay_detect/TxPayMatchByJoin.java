package com.zgg.orderpay_detect;

import com.zgg.orderpay_detect.beans.OrderEvent;
import com.zgg.orderpay_detect.beans.ReceiptEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/*
 * 订单支付实时对账
 *    – 用户下单并支付后，应查询到账信息，进行实时对账
 *    – 如果有不匹配的支付信息或者到账信息，输出提示信息
 *
 * 解决思路
 *    - 用 intervaljoin 连接合并两条流
 * */
public class TxPayMatchByJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 读取数据并转换成POJO类型
        // 读取订单支付事件数据
        String orderFilePath = "E:\\file\\正在进行中\\UserBehaviorAnalysis\\OrderPayDetect\\src\\main\\resources\\OrderLog.csv";
        DataStream<OrderEvent> orderEventStream = env.readTextFile(orderFilePath)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                })
                .filter(data -> !"".equals(data.getTxId()));    // 交易id不为空，必须是pay事件

        // 读取到账事件数据
        String receipFilePath = "E:\\file\\正在进行中\\UserBehaviorAnalysis\\OrderPayDetect\\src\\main\\resources\\ReceiptLog.csv";
        SingleOutputStreamOperator<ReceiptEvent> receiptEventStream = env.readTextFile(receipFilePath)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new ReceiptEvent(fields[0], fields[1], new Long(fields[2]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
                    @Override
                    public long extractAscendingTimestamp(ReceiptEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 区间连接两条流，得到匹配的数据
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> resultStream = orderEventStream
                .keyBy(OrderEvent::getTxId)
                .intervalJoin(receiptEventStream.keyBy(ReceiptEvent::getTxId))
                .between(Time.seconds(-3), Time.seconds(5))    // -3，5 区间范围
                .process(new TxPayMatchDetectByJoin());

        resultStream.print();

        env.execute("tx pay match by join job");
    }

    // 实现自定义ProcessJoinFunction
    public static class TxPayMatchDetectByJoin extends ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>{
        @Override
        public void processElement(OrderEvent left, ReceiptEvent right, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            out.collect(new Tuple2<>(left, right));
        }
    }
}
