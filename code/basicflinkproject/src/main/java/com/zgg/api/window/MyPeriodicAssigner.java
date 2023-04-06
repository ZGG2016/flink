package com.zgg.api.window;

import com.zgg.api.beans.SensorReading;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

// 自定义周期性时间戳分配器
public class MyPeriodicAssigner implements AssignerWithPeriodicWatermarks<SensorReading> {

    private Long bound = 60 * 1000L; // 延迟一分钟
    private Long maxTs = Long.MIN_VALUE; // 当前最大时间戳
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(maxTs - bound);
    }
    @Override
    public long extractTimestamp(SensorReading element, long previousElementTimestamp)
    {
        maxTs = Math.max(maxTs, element.getTimestamp());
        return element.getTimestamp();
    } }
