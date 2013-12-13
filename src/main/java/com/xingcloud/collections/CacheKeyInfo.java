package com.xingcloud.collections;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 12/12/13
 * Time: 11:57 AM
 * To change this template use File | Settings | File Templates.
 */
public class CacheKeyInfo{
  public String type;
  public long startDay;
  public long endDay;
  public String segment;
  public String timeUnitType;
  public String ref;
  public CacheKeyInfo(String type,long startDay,long endDay,String segment,String timeUnitType,String ref){
    this.type=type;
    this.startDay=startDay;
    this.endDay=endDay;
    this.segment=segment;
    this.timeUnitType=timeUnitType;
    this.ref=ref;
  }
  @Override
  public int hashCode(){
    final int prime = 31;
    int result = 1;
    result = prime * result + (int)startDay;
    result = prime * result + (int)(endDay ^ (endDay >>> 32));
    result = prime * result + (type.hashCode() ^ (type.hashCode() >>> 32));
    result = prime * result + (segment.hashCode() ^ (segment.hashCode() >>> 32));
    result = prime * result + (timeUnitType.hashCode() ^ (timeUnitType.hashCode() >>> 32));
    result = prime * result + (ref.hashCode() ^ (ref.hashCode() >>> 32));
    return result;
  }
  public boolean equals(Object o){
    if(this==o)
      return true;
    if(o instanceof CacheKeyInfo){
      CacheKeyInfo cacheKeyInfo=(CacheKeyInfo)o;
      return startDay==cacheKeyInfo.startDay&&endDay==cacheKeyInfo.endDay&&
        type.equals(cacheKeyInfo.type)&&segment.equals(cacheKeyInfo.segment);
    }
    return false;
  }
}