package com.zgg.hotitems_analysis.beans;

// 定义结果输出类型
public class ItemViewCount {
    private Long itemId;
    private Long windowEnd;  // 商品所在窗口的末尾时间
    private Long count;      // 商品在这个窗口的计数

    public ItemViewCount() {
    }

    public ItemViewCount(Long itemId, Long windowEnd, Long count) {
        this.itemId = itemId;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "com.zgg.hotitems_analysis.beans.ItemViewCount{" +
                "itemId=" + itemId +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}
