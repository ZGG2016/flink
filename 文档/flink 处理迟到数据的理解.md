# flink 处理迟到数据的理解

[TOC]

```java
// 事件时间
env.setStreamTimeCharacteristic( TimeCharacteristic.EventTime );

// 最大的延迟 2s
.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2));

// 滚动窗口大小5s
.timeWindow(Time.seconds(5));

// 允许的延迟 1min 
.allowedLateness(Time.minutes(1));
```

#### 1 并行度为 1 时

idea 输出结果：

```sh
# 1547718200 是所属的窗口的起点，窗口范围是[1547718200, 1547718205)、[1547718205, 1547718210)
data> SensorReading{id='sensor_1', timestamp=1547718200, temperature=35.9} 
data> SensorReading{id='sensor_1', timestamp=1547718201, temperature=36.0}
data> SensorReading{id='sensor_1', timestamp=1547718202, temperature=36.1}
data> SensorReading{id='sensor_1', timestamp=1547718203, temperature=36.2}
data> SensorReading{id='sensor_1', timestamp=1547718204, temperature=36.3}
data> SensorReading{id='sensor_1', timestamp=1547718205, temperature=36.4}
data> SensorReading{id='sensor_1', timestamp=1547718206, temperature=36.5}
# 输入这条数据，此时 watermark 为 205，触发窗口计算触发了 [1547718200, 1547718205) 窗口的计算，输出最小温度
# 因为水印设的是2秒，因为设置水印后，由水印的时间戳触发窗口计算
data> SensorReading{id='sensor_1', timestamp=1547718207, temperature=36.6}  
minTemp> SensorReading{id='sensor_1', timestamp=1547718200, temperature=35.9}
# 输入这条数据，更新最小温度
# 此时窗口还未关闭，因为设置了 allowedLateness 为一分钟
data> SensorReading{id='sensor_1', timestamp=1547718203, temperature=33.1}  
minTemp> SensorReading{id='sensor_1', timestamp=1547718203, temperature=33.1}
# 过了一分钟了 【注意：是event time】，此时 [1547718205, 1547718210) 窗口关闭，触发计算，输出这个窗口的最小温度
data> SensorReading{id='sensor_1', timestamp=1547718267, temperature=33.4}  
minTemp> SensorReading{id='sensor_1', timestamp=1547718205, temperature=36.4} 
# 输入这条数据，输出  [1547718200, 1547718205) 窗口的迟到数据
data> SensorReading{id='sensor_1', timestamp=1547718203, temperature=29.0}  
late> SensorReading{id='sensor_1', timestamp=1547718203, temperature=29.0}     
```

```java
    // 计算窗口起点
	public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
		return timestamp - (timestamp - offset + windowSize) % windowSize;
	}

	// 1547718200 - (1547718200 - 0 + 5) % 5 = 0
```

更详细理解窗口起点：[https://www.bilibili.com/video/BV1qy4y1q728?p=57&vd_source=554a818fa3b2b8eac72efab5838146bf](https://www.bilibili.com/video/BV1qy4y1q728?p=57&vd_source=554a818fa3b2b8eac72efab5838146bf)

#### 2 并行度为 2 时

**先要理解 watermark 的传递：[https://www.bilibili.com/video/BV1qy4y1q728?p=53](https://www.bilibili.com/video/BV1qy4y1q728?p=53)**

**详细解释：[https://www.bilibili.com/video/BV1qy4y1q728?p=63&spm_id_from=pageDriver](https://www.bilibili.com/video/BV1qy4y1q728?p=63&spm_id_from=pageDriver)**

指示 data 的一行表示，输出的原始数据。

指示 minTemp 的一行表示，输出的计算后的数据。

```sh
# 以最小的 watermark 为准
data:2> SensorReading{id='sensor_1', timestamp=1547718200, temperature=35.9}
data:1> SensorReading{id='sensor_1', timestamp=1547718201, temperature=36.0}
data:2> SensorReading{id='sensor_1', timestamp=1547718202, temperature=36.1}
data:1> SensorReading{id='sensor_1', timestamp=1547718203, temperature=36.2}
data:2> SensorReading{id='sensor_1', timestamp=1547718204, temperature=36.3}
data:1> SensorReading{id='sensor_1', timestamp=1547718205, temperature=36.4}
data:2> SensorReading{id='sensor_1', timestamp=1547718206, temperature=36.5}
# 线程1：时间戳为 1547718207 时，其 watermark 为205，触发窗口计算
data:1> SensorReading{id='sensor_1', timestamp=1547718207, temperature=36.6}
# 线程2：时间戳为 1547718208 时，其 watermark 为206，此时最小的 watermark 是205，
# 所以这个线程的触发水印也是 205，触发窗口操作，输出最小温度
data:2> SensorReading{id='sensor_1', timestamp=1547718208, temperature=36.7}
minTemp:2> SensorReading{id='sensor_1', timestamp=1547718200, temperature=35.9}
data:1> SensorReading{id='sensor_1', timestamp=1547718209, temperature=36.8}
data:2> SensorReading{id='sensor_1', timestamp=1547718210, temperature=36.9}
data:1> SensorReading{id='sensor_1', timestamp=1547718211, temperature=37.0}
data:2> SensorReading{id='sensor_1', timestamp=1547718212, temperature=37.1}
data:1> SensorReading{id='sensor_1', timestamp=1547718213, temperature=37.2}
minTemp:2> SensorReading{id='sensor_1', timestamp=1547718205, temperature=36.4}
```

```sh
data:1> SensorReading{id='sensor_1', timestamp=1547718200, temperature=35.9}
data:2> SensorReading{id='sensor_1', timestamp=1547718201, temperature=36.0}
data:1> SensorReading{id='sensor_1', timestamp=1547718207, temperature=36.6}
data:2> SensorReading{id='sensor_1', timestamp=1547718208, temperature=36.7}
minTemp:2> SensorReading{id='sensor_1', timestamp=1547718200, temperature=35.9}
data:1> SensorReading{id='sensor_1', timestamp=1547718209, temperature=36.8}
data:2> SensorReading{id='sensor_1', timestamp=1547718267, temperature=34.4}
# 过了一分钟了 【注意：是event time】，同时触发了 [1547718205, 1547718210) 窗口的计算，输出这个窗口的最小温度
data:1> SensorReading{id='sensor_1', timestamp=1547718268, temperature=34.5}
minTemp:2> SensorReading{id='sensor_1', timestamp=1547718207, temperature=36.6}
# 输入这条数据，输出 [1547718200, 1547718205) 窗口的迟到数据
data:2> SensorReading{id='sensor_1', timestamp=1547718203, temperature=29.0}
late:2> SensorReading{id='sensor_1', timestamp=1547718203, temperature=29.0}
```

#### 3 程序

来自尚硅谷 flink 教程：[https://www.bilibili.com/video/BV1qy4y1q728](https://www.bilibili.com/video/BV1qy4y1q728)

```java
package apitest.window;

import apitest.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class EventTimeWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        env.setStreamTimeCharacteristic( TimeCharacteristic.EventTime );
        env.getConfig().setAutoWatermarkInterval(100);


        // socket文本流
        DataStream<String> socketStream = env.socketTextStream("bigdata101", 7777);

        // 转换成SensorReading类型，分配时间戳和watermark
        DataStream<SensorReading> inputStream = socketStream.map(line -> {
            String[] vals = line.split(",");
            return new SensorReading(vals[0], Long.parseLong(vals[1]), Double.parseDouble(vals[2]));
        })
                // 升序数据设置事件时间和watermark
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
//                    @Override
//                    public long extractAscendingTimestamp(SensorReading element) {
//                        return element.getTimestamp() * 1000L;
//                    }
//                })
                // 乱序数据设置时间戳和watermark
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };

        // 基于事件时间的开窗聚合，统计15秒内温度的最小值
        SingleOutputStreamOperator<SensorReading> minTempStream = inputStream.keyBy("id")
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .minBy("temperature");

        inputStream.print("data");
        minTempStream.print("minTemp");
        minTempStream.getSideOutput(outputTag).print("late");

        env.execute();
    }
}
```

测试数据

```
sensor_1,1547718200,35.9
sensor_1,1547718201,36.0
sensor_1,1547718202,36.1
sensor_1,1547718203,36.2
sensor_1,1547718204,36.3
sensor_1,1547718205,36.4
sensor_1,1547718206,36.5
sensor_1,1547718207,36.6
sensor_1,1547718208,36.7
sensor_1,1547718209,36.8
sensor_1,1547718210,36.9
sensor_1,1547718211,37.0
sensor_1,1547718212,37.1
sensor_1,1547718213,37.2
sensor_1,1547718214,37.3
sensor_1,1547718215,37.4
sensor_1,1547718216,37.5
sensor_1,1547718217,37.6
sensor_1,1547718218,37.7
sensor_1,1547718219,37.8
sensor_1,1547718220,37.9
sensor_1,1547718221,38.0
sensor_1,1547718222,38.1
sensor_1,1547718223,38.2
sensor_1,1547718224,38.3
sensor_1,1547718225,38.4
sensor_1,1547718226,38.5
sensor_1,1547718227,38.6
sensor_1,1547718228,38.7
sensor_1,1547718229,38.8
```

----------------------------------------

参考：[尚硅谷flink教程](https://www.bilibili.com/video/BV1qy4y1q728)