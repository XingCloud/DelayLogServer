package com.xingcloud.delayserver;

import java.util.Set;

/**
 * User: IvyTang
 * Date: 13-1-9
 * Time: 下午4:57
 */
public class EventCountSumUid {

    private long date;

    private long count;

    private long sum;

    private Set<String> uidSet;

    public EventCountSumUid(long date, long count, long sum, Set<String> uidSet) {
        this.date = date;
        this.count = count;
        this.sum = sum;
        this.uidSet = uidSet;
    }


    public long getDate() {
        return date;
    }

    public long getCount() {
        return count;
    }

    public long getSum() {
        return sum;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void addCount(long addcount) {
        count += addcount;

    }

    public void addSum(long addsum) {
        sum += addsum;
    }

    public Set<String> getUidSet() {
        return uidSet;
    }

    public void setUidSet(Set<String> uidSet) {
        this.uidSet = uidSet;
    }

    public void addUids(Set<String> uidSet) {
        this.uidSet.addAll(uidSet);
    }


    @Override
    public String toString() {
        return "EventCountSumUid{" +
                "date=" + date +
                ", count=" + count +
                ", sum=" + sum +
                ", uidSetSize=" + uidSet.size() +
                '}';
    }
}
