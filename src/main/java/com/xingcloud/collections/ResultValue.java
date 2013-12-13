package com.xingcloud.collections;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 12/12/13
 * Time: 4:35 PM
 * To change this template use File | Settings | File Templates.
 */
public class ResultValue {
  public String cacheKey;
  public long date;
  public long count,sum;
  public long addCount,addSum;
  public ResultValue(String cacheKey,long date,long count,long sum,long addCount,long addSum){
      this.cacheKey=cacheKey;
      this.date=date;
      this.count=count;
      this.sum=sum;
      this.addCount=addCount;
      this.addSum=addSum;
  }

}
