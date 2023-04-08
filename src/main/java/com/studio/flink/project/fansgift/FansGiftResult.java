package com.studio.flink.project.fansgift;

/**
 * 输出数据类型
 */
public class FansGiftResult {
    public String hostId;
    public String fansId;
    public long giftCount;
    public long windowEnd;

    public FansGiftResult() {
    }

    public FansGiftResult(String hostId, String fansId, long giftCount, long windowEnd) {
        this.hostId = hostId;
        this.fansId = fansId;
        this.giftCount = giftCount;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "FansGiftResult{" +
                "hostId='" + hostId + '\'' +
                ", fansId='" + fansId + '\'' +
                ", giftCount=" + giftCount +
                ", windowEnd=" + windowEnd +
                '}';
    }
}
