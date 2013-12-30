package com.xingcloud.collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 12/12/13
 * Time: 10:14 AM
 * To change this template use File | Settings | File Templates.
 */
public class OrignalData {
   private static Log LOG= LogFactory.getLog(OrignalData.class);
   public Map<FilterKey,List<CacheKeyInfo>> redisCacheKeys;
   private static OrignalData instance=null;
   private OrignalData(){
     redisCacheKeys=new HashMap<FilterKey, List<CacheKeyInfo>>();
   }
   public static OrignalData getInstance(){
     if(instance==null)
        instance=new OrignalData();
     return instance;
   }
   public void clear(){
     redisCacheKeys.clear();
   }
   public synchronized void addCacheKey(FilterKey filterKey,CacheKeyInfo cacheKeyInfo){
//     if(!redisCacheKeys.containsKey(filterKey)){
//        LOG.info("add filter key "+filterKey.pid+"--"+filterKey.eventPattern);
//     }
     List<CacheKeyInfo> cacheKeyInfos=redisCacheKeys.get(filterKey);
     if(cacheKeyInfos==null){
       cacheKeyInfos=new ArrayList<CacheKeyInfo>();
       redisCacheKeys.put(filterKey,cacheKeyInfos);
     }
     cacheKeyInfos.add(cacheKeyInfo);

   }



}
