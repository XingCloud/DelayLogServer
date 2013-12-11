package com.xingcloud.delayserver;

/**
 * User: IvyTang
 * Date: 13-1-9
 * Time: 下午2:24
 */
public class UidValue {

    private int uid = 0;
    private long value = 0l;
    private long timestamp = 0l;

    public UidValue(int uid, long value, long timestamp) {
        this.uid = uid;
        this.value = value;
        this.timestamp = timestamp;
    }

    public int getUid() {
        return uid;
    }

    public long getValue() {
        return Math.abs(value);
//        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + uid;
        result = prime * result + (int) (value ^ (value >>> 32));
        result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o instanceof UidValue) {
            UidValue tmp = (UidValue) o;
            return tmp.getUid() == this.uid && tmp.getValue() == this.value && tmp.getTimestamp() == this.timestamp;
        }
        return false;
    }


}
