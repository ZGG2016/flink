package com.zgg.api.state;

import com.zgg.api.beans.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class KeyedStateApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inStream = env.readTextFile("src/main/resources/sensor.txt");
        DataStream<SensorReading> dataStream = inStream.map(line -> {
            String[] valArr = line.split(",");
            return new SensorReading(valArr[0], Long.parseLong(valArr[1]), Double.parseDouble(valArr[2]));
        });
        // 定义一个flatmap操作，检测温度跳变，输出报警
        DataStream<Tuple3<String,Double,Double>> resStream = dataStream.keyBy("id")
                .flatMap(new TempChangeWarning(10.0));

        resStream.print();
        env.execute();
    }

    public static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String,Double,Double>>{
        private Double thredhold;

        public TempChangeWarning(Double thredhold) {
            this.thredhold = thredhold;
        }

        // 定义状态，保存上一次的温度值
        private ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp-warning",Double.class));
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            Double lastTemp = lastTempState.value();
            if (lastTemp!=null){
                double diff = Math.abs(lastTemp - value.getTemperature());
                if (diff > thredhold){
                    out.collect(new Tuple3<>(value.getId(), value.getTemperature(), lastTemp));
                }
            }
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }
}
