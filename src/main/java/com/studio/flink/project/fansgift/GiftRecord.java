package com.studio.flink.project.fansgift;

/**
 * kafka中传入的数据类型
 */
public class GiftRecord {
    public String hostId;   //主播ID
    public String fansId;   //粉丝ID
    public int giftCount;  //礼物数量
    public long giftTime; //送礼物时间，时间格式：yyyy-MM-dd HH:mm:ss

    public GiftRecord() {
    }

    public GiftRecord(String hostId, String fansId, int giftCount, long giftTime) {
        this.hostId = hostId;
        this.fansId = fansId;
        this.giftCount = giftCount;
        this.giftTime = giftTime;
    }

    @Override
    public String toString() {
        return "GiftRecord{" +
                "hostId='" + hostId + '\'' +
                ", fansId='" + fansId + '\'' +
                ", giftCount=" + giftCount +
                ", giftTime=" + giftTime +
                '}';
    }
}
