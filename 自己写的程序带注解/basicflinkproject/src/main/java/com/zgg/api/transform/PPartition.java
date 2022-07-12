package com.zgg.api.transform;

import com.zgg.api.beans.SensorReading;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<String> inDataStream = env.readTextFile("src/main/resources/sensor.txt");
        DataStream<SensorReading> dataStream = inDataStream.map(line -> {
            String[] valArr = line.split(",");
            return new SensorReading(valArr[0], Long.parseLong(valArr[1]), Double.parseDouble(valArr[2]));
        });
        dataStream.print("input");
        // 1. 广播，将一条数据发到下游的每个分区。
        // 数据源7条，并行度是3，所以输出21条数据
//         dataStream.broadcast().print("broadcast");
        // 2. 随机向下游发数据
        // 数据源7条，所以输出也是7条数据
//        dataStream.shuffle().print("shuffle");
        // 3. 直接在原来的线程上执行下个算子
//        dataStream.forward().print("forward");
        // 4. 每个分区中的数据以轮询方式，均匀的分给下游算子
//        dataStream.rebalance().print("rebalance");
        // 5. 先将分区分组，再以轮询方式，均匀的分给下游算子（详细见源码）
//        dataStream.rescale().print("rescale");
        // 6. 全都发给第一个分区
        // dataStream.global().print("global");
        // 7. 自定义分区
        dataStream.partitionCustom(new MyPartitioner(), "temperature").print("partitionCustom");

        env.execute();
    }

    public static class MyPartitioner implements Partitioner<Double>{

        @Override
        public int partition(Double key, int numPartitions) {
            return key > 30.0 ? 0 : 1;
//            return key > 30.0 ? 1 : 2;
        }
    }
}
