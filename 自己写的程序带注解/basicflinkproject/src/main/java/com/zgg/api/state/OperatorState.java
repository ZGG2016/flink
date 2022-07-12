package com.zgg.api.state;

import com.zgg.api.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

public class OperatorState {
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
        DataStream<Integer> resStream = dataStream.map(new MyCountMapper());

        resStream.print();
        env.execute();
    }

    // 如果不实现 ListCheckpointed 接口，只定义一个本地变量，那么故障时就无法恢复
    // 所以这个接口的作用就是故障恢复，它会取当前的状态，保存下来(snapshotState)，当故障恢复时，就从中恢复状态(restoreState)。
    public static class MyCountMapper implements MapFunction<SensorReading, Integer>, ListCheckpointed<Integer>{

        private Integer count = 0;

        @Override
        public Integer map(SensorReading value) throws Exception {
            count++;
            return count;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            // 每次取当前状态时候，都是返回一个List
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            // 注意理解这个方法
            // Restores the state of the function or operator to that of a previous checkpoint.
            // 将函数或算子的状态恢复到前一个检查点的状态。
            for (Integer num:state){
                count += num;
            }
        }
    }
}
