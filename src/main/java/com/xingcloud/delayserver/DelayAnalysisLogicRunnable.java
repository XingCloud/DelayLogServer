package com.xingcloud.delayserver;

import com.xingcloud.collections.*;
import com.xingcloud.delayserver.redisutil.RedisShardedPoolResourceManager;
import com.xingcloud.delayserver.util.Constants;
import com.xingcloud.dumpredis.DumpRedis;
import com.xingcloud.util.HashFunctions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.ShardedJedis;

import java.sql.*;
import java.text.ParseException;
import java.util.*;

/**
 * User: IvyTang
 * Date: 13-1-8
 * Time: 下午6:18
 */
public class DelayAnalysisLogicRunnable implements Runnable {

  private static Log LOG = LogFactory.getLog(DelayAnalysisLogicRunnable.class);

  //<pid,<event,<date,uidvalue>>>
  private Map<String, Map<String, Map<Long, Set<UidValue>>>> delayLogs;
  private Map<String, Set<String>> delayEvents;

  private String KEYCACHE_DB = "redis_cachekey";

  private String KEYCACHE_DB_USER = "cachekey";

  private String KEYCACHE_DB_PWD = "23XFdx5";

  private int FILTER_ONCE_CHECK = 1000;

  private DumpRedis dumpRedis=new DumpRedis();


  public DelayAnalysisLogicRunnable(Map<String, Map<String, Map<Long, Set<UidValue>>>> delayLogs,
                                    Map<String, Set<String>> delayEvents) {
    this.delayLogs = delayLogs;
    this.delayEvents = delayEvents;
  }

  @Override
  public void run() {
    LOG.info("enter DelayAnalysisLogicRunnable...");


    try {
      LOG.info("=====" + delayLogs.size());
      buildFilterDelayEventRelationship();
      Map<String, EventCountSumUid> results = analysisLogs();
      putHandleCacheValueInMem(results);
      LOG.info("=====" + delayLogs.size());
    } catch (Throwable e) {
      LOG.error(e.toString(), e);
      LOG.error(e.getMessage());
    }
  }


  private String filterSpecialCharacters(String s) {
    s = s.replace("'", "");
    s = s.replace("\"", "");
    s = s.replace("\\", "");
    return s;
  }


  //构建每个项目的filter和延迟event的对应关系，*.*的filter不构建对应关系
  private void buildFilterDelayEventRelationship() {
    long currentTime = System.currentTimeMillis();
    try {
      for (FilterKey filterKey : OrignalData.getInstance().redisCacheKeys.keySet()) {
        String pid = filterKey.pid;
        String filter = filterKey.eventPattern;
        if (!filter.equals("*.*")) {
          Set<String> events = delayEvents.get(pid);
          LevelEvent levelEventPattern = new LevelEvent(filter);
          for (String event : events) {
            LevelEvent levelEvent = new LevelEvent(event);
            if (levelEventPattern.contains(levelEvent))
              FilterDelayEventRelationShip.getInstance().addRelation(pid, event, filterKey);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("buildFilterDelayEventRelationship error.", e);
    }
    LOG.info("buildFilterDelayEventRelationship completed.using " + (System.currentTimeMillis() - currentTime) + "ms.");
  }




  //分析每条延迟log,找出对应的redis里面cache，处理这条延迟log对应的所有cache key的值
  private Map<String, EventCountSumUid> analysisLogs() {
    LOG.info("enter analysisLogs...");
    long currentTime = System.currentTimeMillis();
    Map<String, EventCountSumUid> results = new HashMap<String, EventCountSumUid>();
    for (Map.Entry<String, Map<String, Map<Long, Set<UidValue>>>> entry : delayLogs.entrySet()) {
      String pid = entry.getKey();
      Map<String, Map<Long, Set<UidValue>>> pMap = entry.getValue();
      for (Map.Entry<String, Map<Long, Set<UidValue>>> pEntry : pMap.entrySet()) {
        String event = pEntry.getKey();
        List<FilterKey> filters = getFilters(pid, event);
        Map<Long, Set<UidValue>> dateUidValues = pEntry.getValue();
        for (Map.Entry<Long, Set<UidValue>> duvEntry : dateUidValues.entrySet()) {
          long date = duvEntry.getKey();
          long eventCount = duvEntry.getValue().size();
          long eventSum = 0;
          Set<String> uids = new HashSet<String>();
          //LOG.info(entry.getKey() + "\t" + pEntry.getKey() + "\t" + duvEntry.getKey() + "\t" + duvEntry
          //        .getValue().size());
          for (UidValue uidValue : duvEntry.getValue()) {

            //TODO
            if (uidValue == null) {
              LOG.error("NPE " + pid + "\t" + event + "\t" + date);
              continue;
            }
            eventSum += uidValue.getValue();
            uids.add(String.valueOf(uidValue.getUid()));
          }
          for (FilterKey filter : filters) {
            List<String> caches = getCaches(date, filter);
            for (String cache : caches) {
              buildEventCountSumUidToResult(results, cache, date, eventCount, eventSum, uids);
            }
          }

          String allEventCountKey = buildAllEventCountCacheKey(pid, date);
          buildEventCountSumUidToResult(results, allEventCountKey, date, eventCount, 0, new HashSet<String>());
        }
      }
    }
    LOG.info("analysisLogs completed.using " + (System.currentTimeMillis() - currentTime) + "ms.");
    return results;
  }

  private void buildEventCountSumUidToResult(Map<String, EventCountSumUid> results, String cacheKey,
                                             long date, long eventCount, long eventSum, Set<String> newUids) {
    EventCountSumUid eventCountSumUid = results.get(cacheKey);
    if (eventCountSumUid == null) {
      eventCountSumUid = new EventCountSumUid(date, 0l, 0l, new HashSet<String>());
      results.put(cacheKey, eventCountSumUid);
    }
    eventCountSumUid.addCount(eventCount);
    eventCountSumUid.addSum(eventSum);
    eventCountSumUid.addUids(newUids);

  }


  private String buildAllEventCountCacheKey(String pid, long date) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("COMMON,");
    stringBuilder.append(pid);
    stringBuilder.append(",");
    stringBuilder.append(dateFormatToRedisFormat(String.valueOf(date)));
    stringBuilder.append(",");
    stringBuilder.append(dateFormatToRedisFormat(String.valueOf(date)));
    stringBuilder.append(",*.*,TOTAL_USER,VF-ALL-0-0,PERIOD");
    return stringBuilder.toString();

  }

  private List<FilterKey> getFilters(String pid, String event) {
    List<FilterKey> filters = new ArrayList<FilterKey>();
    try {
      filters = FilterDelayEventRelationShip.getInstance().relationShip.get(pid).get(event);
    } catch (Exception e) {
      LOG.error("getDelayEventID errors. " + e.getMessage());
    }
    return filters;
  }

  private List<String> getCaches(long date, FilterKey filterKey) {
    List<String> caches = new ArrayList<String>();
    try {
      List<CacheKeyInfo> cacheKeyInfos = OrignalData.getInstance().redisCacheKeys.get(filterKey);
      for (CacheKeyInfo cacheKeyInfo : cacheKeyInfos) {
        if (cacheKeyInfo.startDay <= date && cacheKeyInfo.endDay >= date) {
          caches.add(cacheKeyInfo.type + "," + filterKey.pid + "," +
            dateFormatToRedisFormat(String.valueOf(cacheKeyInfo.startDay)) + "," +
            dateFormatToRedisFormat(String.valueOf(cacheKeyInfo.endDay)) + "," +
            filterKey.eventPattern + "," +
            cacheKeyInfo.segment + "," +
            "VF-ALL-0-0" + "," +
            cacheKeyInfo.timeUnitType
            + cacheKeyInfo.ref != null ? cacheKeyInfo.ref : "");
        }
      }
    } catch (Exception e) {
      LOG.error("getDelayEventID errors. " + e.getMessage());
    }
    return caches;
  }


  private void putHandleCacheValueInMem(Map<String, EventCountSumUid> results) {
    long currentTime = System.currentTimeMillis();
    ShardedJedis shardedRedis = null;
    try {
      shardedRedis = RedisShardedPoolResourceManager.getInstance().getCache(Constants.REDIS_CACHE_DB);
      for (Map.Entry<String, EventCountSumUid> entry : results.entrySet()) {
        LOG.info("delay log:" + entry.getKey() + ":" + entry.getValue().toString());

        //sof-dsk & sof-newgdp暂不分析延迟
        if (entry.getKey().contains("sof-dsk") || entry.getKey().contains("sof-newgdp"))
          continue;

        String value = null;
        Map<String, String> stringMap = shardedRedis.hgetAll(entry.getKey());
        for (Map.Entry<String, String> mapEntry : stringMap.entrySet()) {
          if (!mapEntry.getKey().equals("TIMESTAMP")) {
            value = mapEntry.getValue();
          }
          LOG.info(mapEntry.getKey() + ":" + mapEntry.getValue());
        }
        if (value != null) {
          String[] tmps = value.split("#");
          long count = tmps[0].length() == 0 ? Long.MAX_VALUE : Long.valueOf(tmps[0]);
          long sum = tmps[1].length() == 0 ? Long.MAX_VALUE : Long.valueOf(tmps[1]);
          if (entry.getKey().contains("visit.*") && count == Long.MAX_VALUE & sum == Long.MAX_VALUE) {
            //如果事件是visit.*，且需要的是uidNum，就把redis里面的uidNum赋给count，这个延迟log的count属性变为uidSum
            count = tmps[2].length() == 0 ? Long.MAX_VALUE : Long.valueOf(tmps[2]);
            LOG.info("switch:" + entry.getValue().getCount() + "\t" + entry.getValue().getUidSet().size());
            entry.getValue().setCount(entry.getValue().getUidSet().size());

          }
          LOG.info("hgetAll from redis:" + entry.getKey() + "\t" + entry.getValue().toString() +
            "\tredis value" + value + " count:" + count + "\tsum:" +
            sum);

          anaResultInMem(shardedRedis, HashFunctions.md5(entry.getKey().getBytes()),
            entry.getKey(),
            entry.getValue().getDate(), count, sum, entry.getValue().getCount(),
            entry.getValue().getSum());
        }
      }
    } catch (Exception e) {
      LOG.error("changeCacheValueInRedis" + e.getMessage(), e);
      RedisShardedPoolResourceManager.getInstance().returnBrokenResource(shardedRedis);
      shardedRedis = null;
    } finally {
      RedisShardedPoolResourceManager.getInstance().returnResource(shardedRedis);
    }
    LOG.info("changeCacheValueInRedis completed.using " + (System.currentTimeMillis() - currentTime) + "ms.");
  }

  //首先检查这3个小时的延迟log数量是否满足过期redis的条件：
  //  如果>=0.05,删除mysql的记录，并置redis的key过期
  //  没达到过期条件，查询这条redis key在mysql里面的状态：
  //      不存在，将这条记录的在redis里面的count&sum和这三小时的延迟log的addCount&addSum添加进mysql
  //      存在，则又分两种情况：
  //          mysql里面存的count&sum与redis里面的count&sum不一致，说明redis的缓存被删过，之前的addCount和addSum失效；
  //          count&sum更新为从redis里面查出来的新的count&sum，addCount&addSum为这三个小时的addCount&addSum
  //          一致，再判断mysql里面存的历史addCount&addSum，加上这3小时的addCount&addSum：
  //              达到>=0.05的条件，删除mysql的记录，并置redis的key过期
  //              没有0.05，更新mysql的addCount&addSum
  private void anaResultInMem(ShardedJedis shardedRedis, long keyId, String key, long date,
                              long cacheCount, long cacheSum, long addCount, long addSum) {

    try {

      if (checkIfExpireRedisKey(key, cacheCount, cacheSum, addCount, addSum)) {
        LOG.info("===delCacheInMySQLCache===directly reache the ratio. " + key + "\t" + cacheCount + "\t" +
          cacheSum + "\t" + addCount + "\t" + addSum);
        delCacheInMemCache(shardedRedis, keyId, key);
        return;
      }

      if (AnaResultTable.getInstance().resultValueMap.get(keyId) != null) {
        ResultValue resultValue = AnaResultTable.getInstance().resultValueMap.get(keyId);
        if (resultValue.count != cacheCount || resultValue.sum != cacheSum) {
          resultValue.count = cacheCount;
          resultValue.sum = cacheSum;
          resultValue.addCount = addCount;
          resultValue.addSum = addSum;
        } else {
          if (checkIfExpireRedisKey(key, cacheCount, cacheSum, resultValue.addCount + addCount, resultValue.addSum + addSum)) {
            LOG.info("===delCacheInMySQLCache===mysql+redis addcount/addsum reach the ratio." + key + "redis " +
              "count&sum:" + cacheCount + "\t" + cacheSum + " mysql+redis addcount/addsum:" +
              (resultValue.addCount + addCount) + "\t" + (resultValue.addSum + addSum));
            delCacheInMemCache(shardedRedis, keyId, key);
          } else {
            resultValue.addCount += addCount;
            resultValue.addSum += addSum;
          }
        }
      } else {
        LOG.info("new result.insert into mysql. " + key + "\t" + cacheCount + "\t" + cacheSum + "\t" +
          addCount + "\t" + addSum);
        AnaResultTable.getInstance().addResult(key, date, cacheCount, cacheSum, addCount, addSum);
      }
    } catch (Exception e) {
      LOG.error("anaResultInMysql " + e.getMessage(), e);
    }
  }

  private boolean checkIfExpireRedisKey(String key, long count, long sum, long addCount, long addSum) {
    if (key.contains("pay.") || key.contains("adcalc.")) {
      return ((float) addCount / (float) count >= Constants.PAY_ADCALC_DELAY_RATIO) || ((float) addSum / (float) sum >= Constants
        .PAY_ADCALC_DELAY_RATIO);
    } else
      return ((float) addCount / (float) count >= Constants.DELAY_RATIO) || ((float) addSum / (float) sum >= Constants
        .DELAY_RATIO);
  }

  private void delCacheInMemCache(ShardedJedis shardedRedis,
                                  long keyID, String key) throws Exception {

    AnaResultTable.getInstance().remove(keyID);
    shardedRedis.del(key);
    //如果是所有事件计数器，就没有segment和细分
    if (key.contains("*.*"))
      return;
    //TODO 删除这一个cache对应的所有cache,带segment和细分的
    Set<String> relatedKeys = null;
    try {
      relatedKeys = getRelatedKeys(key);
    } catch (Exception e) {
      LOG.error("getRelatedKeys ERROR " + e.getMessage(), e);
    }
    if (relatedKeys != null)
      for (String rKey : relatedKeys) {
        LOG.info("delete relate key :" + rKey);
        shardedRedis.del(rKey);
      }
  }


  private String dateFormatToRedisFormat(String date) {
    return date.substring(0, 4) + "-" + date.substring(4, 6) + "-" + date.substring(6);
  }

  private Connection getConnection() throws SQLException {
    return DriverManager.getConnection("jdbc:mysql://192.168.1.134:3306/" + KEYCACHE_DB,
      KEYCACHE_DB_USER, KEYCACHE_DB_PWD);
  }

  private void clearResultStatement(ResultSet resultSet, Statement statement) {
    try {
      if (resultSet != null)
        resultSet.close();
      if (statement != null)
        statement.close();
    } catch (SQLException e) {
      LOG.error(e.getMessage());
    }
  }

  private void closeConnection(Connection connection) {

    try {
      if (connection != null)
        connection.close();
    } catch (SQLException e) {
      LOG.error(e.getMessage());
    }
  }

  private Set<String> getRelatedKeys(String key) throws ParseException {
    Set<String> keys = new HashSet<String>();
    String[] tmps = key.split(",");
    try {
      FilterKey filterKey = new FilterKey(tmps[1], tmps[4]);
      List<CacheKeyInfo> cacheKeyInfos = OrignalData.getInstance().redisCacheKeys.get(filterKey);
      for (CacheKeyInfo cacheKeyInfo : cacheKeyInfos) {
        String startDay = dateFormatToRedisFormat(String.valueOf(cacheKeyInfo.startDay));
        String endDay = dateFormatToRedisFormat(String.valueOf(cacheKeyInfo.endDay));
        if (startDay.equals(tmps[2]) && endDay.equals(tmps[3])) {
          keys.add(getCacheKeyStr(filterKey, cacheKeyInfo));
        }
      }

    } catch (Exception e) {
      LOG.error("getRelatedKeys error", e);
    }
    return keys;
  }

  private String getCacheKeyStr(FilterKey filterKey, CacheKeyInfo cacheKeyInfo) {
    return
      cacheKeyInfo.type + "," + filterKey.pid + "," +
        dateFormatToRedisFormat(String.valueOf(cacheKeyInfo.startDay)) + "," +
        dateFormatToRedisFormat(String.valueOf(cacheKeyInfo.endDay)) + "," +
        filterKey.eventPattern + "," +
        cacheKeyInfo.segment + "," +
        "VF-ALL-0-0" + "," +
        cacheKeyInfo.timeUnitType
        + cacheKeyInfo.ref != null ? cacheKeyInfo.ref : "";
  }


}
