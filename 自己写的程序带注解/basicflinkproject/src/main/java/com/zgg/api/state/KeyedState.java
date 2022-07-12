package com.zgg.api.state;

import com.zgg.api.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyedState {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inStream = env.readTextFile("src/main/resources/sensor.txt");
        DataStream<SensorReading> dataStream = inStream.map(line -> {
            String[] valArr = line.split(",");
            return new SensorReading(valArr[0], Long.parseLong(valArr[1]), Double.parseDouble(valArr[2]));
        });

        // 定义一个有状态的map操作，统计当前分区数据个数
        DataStream<Integer> resStream = dataStream.keyBy("id")
                .map(new MyKeyedCountMapper());

        resStream.print();
        env.execute();
    }

    // 自定义RichMapFunction
    public static class MyKeyedCountMapper extends RichMapFunction<SensorReading,Integer>{

        private ValueState<Integer> countValueState;
        // 其它类型状态的声明
//        private ListState<String> myListState;
//        private MapState<String, Double> myMapState;
//        private ReducingState<SensorReading> myReducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countValueState = getRuntimeContext().getState( new ValueStateDescriptor<Integer>("value-state",Integer.class,0));

//            myListState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my-list", String.class));
//            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("my-map", String.class, Double.class));
//            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>())

        }

        @Override
        public Integer map(SensorReading value) throws Exception {

            Integer count = countValueState.value();
            count++;
            countValueState.update(count);
            return count;


//            // 其它状态API调用
//            // list state
//            for(String str: myListState.get()){
//                System.out.println(str);
//            }
//            myListState.add("hello");
//            // map state
//            myMapState.get("1");
//            myMapState.put("2", 12.3);
//            myMapState.remove("2");
//            // reducing state
////            myReducingState.add(value);
//
//            myMapState.clear();
        }
    }
}
