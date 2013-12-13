package com.xingcloud.collections;

import com.xingcloud.util.HashFunctions;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 12/12/13
 * Time: 4:42 PM
 * To change this template use File | Settings | File Templates.
 */
public class AnaResultTable {
  public Map<Long,ResultValue> resultValueMap;
  private static AnaResultTable instance=null;
  private AnaResultTable(){
    resultValueMap=new HashMap<Long, ResultValue>();
  }
  public static AnaResultTable getInstance(){
    if(instance==null)
      instance=new AnaResultTable();
    return instance;
  }
  public void clear(){
    resultValueMap.clear();
  }
  public ResultValue remove(long cacheKeyMd5){
    return resultValueMap.remove(cacheKeyMd5);
  }
  public void addResult(String cacheKey,long date,long count,long value,long addCount,long addValue){
    resultValueMap.put(HashFunctions.md5(cacheKey.getBytes()),new ResultValue(cacheKey,date,count,value,addCount,addValue));
  }
}
