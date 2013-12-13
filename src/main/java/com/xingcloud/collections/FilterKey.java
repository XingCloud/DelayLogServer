package com.xingcloud.collections;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 12/12/13
 * Time: 11:56 AM
 * To change this template use File | Settings | File Templates.
 */
public  class FilterKey{
  public String pid;
  public String eventPattern;
  public FilterKey(String pid,String eventPattern){
    this.pid=pid;
    this.eventPattern=eventPattern;
  }
  @Override
  public int hashCode(){
    final int prime = 31;
    int result = 1;
    result = prime * result + pid.hashCode();
    result = prime * result + (eventPattern.hashCode() ^ (eventPattern.hashCode() >>> 32));
    return result;
  }
  @Override
  public boolean equals(Object o){
    if(this==o)
      return true;
    if(o instanceof FilterKey){
      FilterKey filterKey=(FilterKey)o;
      return pid.equals(filterKey.pid)&&eventPattern.equals(filterKey.eventPattern);
    }
    return false;
  }
}
