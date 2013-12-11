package com.xingcloud.delayserver;

import com.xingcloud.delayserver.redisutil.RedisShardedPoolResourceManager;
import com.xingcloud.delayserver.util.Constants;
import com.xingcloud.util.HashFunctions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.ShardedJedis;

import java.sql.*;
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


  public DelayAnalysisLogicRunnable(Map<String, Map<String, Map<Long, Set<UidValue>>>> delayLogs,
                                    Map<String, Set<String>> delayEvents) {
    this.delayLogs = delayLogs;
    this.delayEvents = delayEvents;
  }

  @Override
  public void run() {
    LOG.info("enter DelayAnalysisLogicRunnable...");


    Connection connection = null;
    try {
      LOG.info("=====" + delayLogs.size());
      connection = getConnection();
      pushDelayEventToMySQL(connection);
      Map<Integer, Set<Integer>> relationships = buildFilterDelayEventRelationship(connection);
      pushRelationShipToMySQL(relationships, connection);
      relationships = null;
      Map<String, EventCountSumUid> results = analysisLogs(connection);
      putHandleCacheValueInMySQL(connection, results);
      LOG.info("=====" + delayLogs.size());
    } catch (Throwable e) {
      LOG.error(e.toString(), e);
      LOG.error(e.getMessage());
    } finally {
      closeConnection(connection);
    }
  }


  //把每天延迟log的事件分层存进mysql
  private void pushDelayEventToMySQL(Connection connection) {
    long currentTime = System.currentTimeMillis();
    Statement statement = null;
    try {
      statement = connection.createStatement();
      long delayID = 0;
      for (Map.Entry<String, Set<String>> entry : delayEvents.entrySet()) {
        for (String event : entry.getValue()) {
          StringBuilder stringBuilder = new StringBuilder();
          stringBuilder.append("insert into delayevent(id,pid,");
          String[] tmps = event.split("\\.");
          for (int i = 0; i < tmps.length; i++) {
            stringBuilder.append("l");
            stringBuilder.append(i);
            if (i < tmps.length - 1)
              stringBuilder.append(",");
          }
          stringBuilder.append(") values(");
          stringBuilder.append(delayID);
          stringBuilder.append(",\"");
          stringBuilder.append(entry.getKey());
          stringBuilder.append("\",");
          for (int i = 0; i < tmps.length; i++) {
            stringBuilder.append("\"" + filterSpecialCharacters(tmps[i]) + "\"");
            if (i < tmps.length - 1)
              stringBuilder.append(",");
          }
          stringBuilder.append(")");
          statement.execute(stringBuilder.toString());
          delayID++;
        }
      }
    } catch (SQLException e) {

      LOG.error("pushDelayEventToMySQL error. " + e.getMessage());
    } finally {
      clearResultStatement(null, statement);
    }
    LOG.info("pushDelayEventToMySQL completed.using " + (System.currentTimeMillis() - currentTime) + "ms.");
  }

  private String filterSpecialCharacters(String s) {
    s = s.replace("'", "");
    s = s.replace("\"", "");
    s = s.replace("\\","");
    return s;
  }


  //构建每个项目的filter和延迟event的对应关系，*.*的filter不构建对应关系
  private Map<Integer, Set<Integer>> buildFilterDelayEventRelationship(Connection connection) {
    long currentTime = System.currentTimeMillis();
    Map<Integer, Set<Integer>> relationships = new HashMap<Integer, Set<Integer>>();
    Statement statement = null;
    Statement delayEventStatement = null;
    ResultSet resultSet = null;
    ResultSet delayEventResultSet = null;
    try {
      statement = connection.createStatement();
      delayEventStatement = connection.createStatement();
      resultSet = statement.executeQuery("select count(*) from filter");
      int count = 0;
      if (resultSet.next()) {
        count = resultSet.getInt(1);
      }
      resultSet.close();
      resultSet = null;
      int times = count / FILTER_ONCE_CHECK + 1;
      for (int i = 0; i < times; i++) {
        resultSet = statement.executeQuery("select * from filter limit " + i * FILTER_ONCE_CHECK + "," +
                "" + FILTER_ONCE_CHECK);
        while (resultSet.next()) {
          int filterId = resultSet.getInt(1);
          String pid = resultSet.getString(2);
          String filter = resultSet.getString(3);
          if (!filter.equals("*.*")) {
            StringBuilder queryStringBuilder = new StringBuilder();

            queryStringBuilder.append("select id from delayevent where pid=\"");
            queryStringBuilder.append(pid);
            queryStringBuilder.append("\"");
            String[] eventfilters = filter.split("\\.");
            for (int j = 0; j < eventfilters.length; j++) {
              if (!eventfilters[j].equals("*")) {
                queryStringBuilder.append(" and l");
                queryStringBuilder.append(j);
                queryStringBuilder.append("=\"");
                queryStringBuilder.append(filterSpecialCharacters(eventfilters[j]));
                queryStringBuilder.append("\"");
              }
            }
            try {
              delayEventResultSet = delayEventStatement.executeQuery(queryStringBuilder.toString());
              while (delayEventResultSet.next()) {
                Set<Integer> delayEventIds = relationships.get(filterId);
                if (delayEventIds == null) {
                  delayEventIds = new HashSet<Integer>();
                  relationships.put(filterId, delayEventIds);
                }
                delayEventIds.add(delayEventResultSet.getInt(1));
              }
              delayEventResultSet.close();
              delayEventResultSet = null;
            } catch (Exception e) {
              LOG.error("buildFilterDelayEventRelationship error." + queryStringBuilder.toString(), e);
            }
          }
        }
      }
    } catch (SQLException e) {
      LOG.error("buildFilterDelayEventRelationship error. " + e.getMessage());
    } finally {
      clearResultStatement(delayEventResultSet, delayEventStatement);
      clearResultStatement(resultSet, statement);
    }
    LOG.info("buildFilterDelayEventRelationship completed.using " + (System.currentTimeMillis() - currentTime) + "ms.");
    return relationships;
  }


  //把对应关系存进MySQL
  private void pushRelationShipToMySQL(Map<Integer, Set<Integer>> relationships, Connection connection) {
    long currentTime = System.currentTimeMillis();
    Statement statement = null;
    try {
      statement = connection.createStatement();
      long relationID = 0;
      for (Map.Entry<Integer, Set<Integer>> entry : relationships.entrySet()) {
        for (Integer eventId : entry.getValue()) {
          statement.execute("insert into relationship(id,filterid,eventid) values(" + relationID + "," + entry
                  .getKey() + "," + eventId + ")");
          relationID++;
        }
      }
    } catch (SQLException e) {
      LOG.error("pushRelationShipToMySQL errors. " + e.getMessage());
    } finally {
      clearResultStatement(null, statement);
    }
    LOG.info("pushRelationShipToMySQL completed.using " + (System.currentTimeMillis() - currentTime) + "ms.");
  }


  //分析每条延迟log,找出对应的redis里面cache，处理这条延迟log对应的所有cache key的值
  private Map<String, EventCountSumUid> analysisLogs(Connection connection) {
    LOG.info("enter analysisLogs...");
    long currentTime = System.currentTimeMillis();
    Map<String, EventCountSumUid> results = new HashMap<String, EventCountSumUid>();
    for (Map.Entry<String, Map<String, Map<Long, Set<UidValue>>>> entry : delayLogs.entrySet()) {
      String pid = entry.getKey();
      Map<String, Map<Long, Set<UidValue>>> pMap = entry.getValue();
      for (Map.Entry<String, Map<Long, Set<UidValue>>> pEntry : pMap.entrySet()) {
        String event = pEntry.getKey();
        List<String> filters = getFilters(connection, getDelayEventID(connection, pid, event));
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
          for (String filter : filters) {
            List<String> caches = getCaches(connection, pid, date, filter);
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

  private int getDelayEventID(Connection connection, String pid, String event) {
    Statement statement = null;
    ResultSet resultSet = null;
    try {
      statement = connection.createStatement();

      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append("select id from delayevent where pid=\"");
      stringBuilder.append(pid);
      stringBuilder.append("\"");

      String[] tmps = event.split("\\.");
      for (int i = 0; i < 6; i++) {
        if (i >= tmps.length) {
          stringBuilder.append(" and l");
          stringBuilder.append(i);
          stringBuilder.append(" is null");
        } else {
          stringBuilder.append(" and l");
          stringBuilder.append(i);
          stringBuilder.append("=\"");
          stringBuilder.append(filterSpecialCharacters(tmps[i]));
          stringBuilder.append("\"");
        }
      }
      resultSet = statement.executeQuery(stringBuilder.toString());
      if (resultSet.next())
        return resultSet.getInt(1);
    } catch (SQLException e) {
      LOG.error("getDelayEventID errors. " + e.getMessage());
    } finally {
      clearResultStatement(resultSet, statement);
    }
    return 0;
  }

  private List<String> getFilters(Connection connection, int delayEventID) {
    List<String> filters = new ArrayList<String>();
    Statement statement = null;
    ResultSet resultSet = null;
    try {
      statement = connection.createStatement();
      resultSet = statement.executeQuery("select event from filter INNER JOIN relationship on " +
              "filter.id=relationship.filterid where relationship.eventid=" + delayEventID);
      while (resultSet.next()) {
        filters.add(resultSet.getString(1));
      }
    } catch (SQLException e) {
      LOG.error("getDelayEventID errors. " + e.getMessage());
    } finally {
      clearResultStatement(resultSet, statement);
    }
    return filters;
  }

  private List<String> getCaches(Connection connection, String pid, long date, String filter) {
    List<String> caches = new ArrayList<String>();
    Statement statement = null;
    ResultSet resultSet = null;
    try {
      statement = connection.createStatement();
      resultSet = statement.executeQuery("select type,pid,sdate,edate,filter from cache where type=\"COMMON\" and pid=\"" + pid
              + "\" and sdate<=" + date + " and edate>=" + date + " and filter=\"" + filter + "\" and " +
              "segment=\"TOTAL_USER\" and ref0=\"PERIOD\" and ref1 is null");


      while (resultSet.next()) {
        caches.add(resultSet.getString(1) + "," + resultSet.getString(2) + "," +
                dateFormatToRedisFormat(String.valueOf(resultSet.getInt(3))) + "," +
                dateFormatToRedisFormat(String.valueOf(resultSet.getInt(4))) + "," +
                resultSet.getString(5) + ",TOTAL_USER,VF-ALL-0-0,PERIOD");

      }
    } catch (SQLException e) {
      LOG.error("getDelayEventID errors. " + e.getMessage());
    } finally {
      clearResultStatement(resultSet, statement);
    }
    return caches;
  }


  private void putHandleCacheValueInMySQL(Connection connection, Map<String, EventCountSumUid> results) {
    long currentTime = System.currentTimeMillis();
    ShardedJedis shardedRedis = null;
    try {
      shardedRedis = RedisShardedPoolResourceManager.getInstance().getCache(Constants.REDIS_CACHE_DB);
      for (Map.Entry<String, EventCountSumUid> entry : results.entrySet()) {
        LOG.info("delay log:" + entry.getKey() + ":" + entry.getValue().toString());

        //sof-dsk & sof-newgdp暂不分析延迟
        if(entry.getKey().contains("sof-dsk") || entry.getKey().contains("sof-newgdp"))
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

          anaResultInMysql(connection, shardedRedis, HashFunctions.md5(entry.getKey().getBytes()),
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
  private void anaResultInMysql(Connection connection, ShardedJedis shardedRedis, long keyId, String key, long date,
                                long cacheCount, long cacheSum, long addCount, long addSum) {

    Statement statement = null;
    ResultSet resultSet = null;
    try {

      if (checkIfExpireRedisKey(key, cacheCount, cacheSum, addCount, addSum)) {
        LOG.info("===delCacheInMySQLCache===directly reache the ratio. " + key + "\t" + cacheCount + "\t" +
                cacheSum + "\t" + addCount + "\t" + addSum);
        delCacheInMySQLCache(connection, shardedRedis, keyId, key);
        return;
      }

      statement = connection.createStatement();
      String selectSQL = "select count,sum,addcount,addsum from result where id=" + keyId;
      resultSet = statement.executeQuery(selectSQL);
      if (resultSet.next()) {
        long countInMySQL = resultSet.getLong(1);
        long sumInMySQL = resultSet.getLong(2);
        long addCountInMySQL = resultSet.getLong(3);
        long addSumInMySQL = resultSet.getLong(4);
        if (countInMySQL != cacheCount || sumInMySQL != cacheSum) {
          String updateRecordSQL = String.format("update result set count=%s,sum=%s,addcount=%s," +
                  "addsum=%s where id=%s", cacheCount, cacheSum, addCount, addSum, keyId);
          LOG.info("differ result." + key + "\tmysql count sum:" + countInMySQL + "\t" + sumInMySQL +
                  "redis " + "count sum:" + cacheCount + "\t" + cacheSum);
          statement.executeUpdate(updateRecordSQL);
        } else {
          if (checkIfExpireRedisKey(key, cacheCount, cacheSum, addCountInMySQL + addCount, addSumInMySQL + addSum)) {
            LOG.info("===delCacheInMySQLCache===mysql+redis addcount/addsum reach the ratio." + key + "redis " +
                    "count&sum:" + cacheCount + "\t" + cacheSum + " mysql+redis addcount/addsum:" +
                    (addCountInMySQL + addCount) + "\t" + (addSumInMySQL + addSum));
            delCacheInMySQLCache(connection, shardedRedis, keyId, key);
          } else {
            String updateRecordSQL = String.format("update result set addcount=%s,addsum=%s where id=%s",
                    addCount + addCountInMySQL, addSum + addSumInMySQL, keyId);
            LOG.info("just update mysql. " + key + " redis " + "count&sum:" + cacheCount + "\t" + cacheSum
                    + " mysql+redis addcount/addsum:" + (addCountInMySQL + addCount) + "\t" + (addSumInMySQL + addSum));
            statement.executeUpdate(updateRecordSQL);
          }
        }
      } else {
        String insertSQL = String.format("insert into result values(%s,\"%s\",%s,%s,%s,%s,%s)", keyId, key,
                date, cacheCount, cacheSum, addCount, addSum);
        LOG.info("new result.insert into mysql. " + key + "\t" + cacheCount + "\t" + cacheSum + "\t" +
                addCount + "\t" + addSum);
        statement.execute(insertSQL);
      }
    } catch (Exception e) {
      LOG.error("anaResultInMysql " + e.getMessage(), e);
    } finally {
      clearResultStatement(resultSet, statement);
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

  private void delCacheInMySQLCache(Connection connection, ShardedJedis shardedRedis,
                                    long keyID, String key) throws Exception {

    Statement statement = connection.createStatement();
    statement.executeUpdate("delete from result where id=" + keyID);
    statement.close();


    shardedRedis.del(key);
    //如果是所有事件计数器，就没有segment和细分
    if (key.contains("*.*"))
      return;
    //TODO 删除这一个cache对应的所有cache,带segment和细分的
    Set<String> relatedKeys = null;
    try {
      relatedKeys = getRelatedKeys(connection, key);
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

  private Set<String> getRelatedKeys(Connection connection, String key) {
    Set<String> keys = new HashSet<String>();

    String[] tmps = key.split(",");

    Statement statement = null;
    ResultSet resultSet = null;

    try {
      statement = connection.createStatement();
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append("select * from cache where pid=\"");
      stringBuilder.append(tmps[1]);
      stringBuilder.append("\" and sdate=");
      stringBuilder.append(tmps[2].replaceAll("-", ""));
      stringBuilder.append(" and edate=");
      stringBuilder.append(tmps[3].replaceAll("-", ""));
      stringBuilder.append(" and filter=\"");
      stringBuilder.append(tmps[4]);
      stringBuilder.append("\"");
      LOG.info(stringBuilder.toString());
      resultSet = statement.executeQuery(stringBuilder.toString());
      while (resultSet.next()) {
        StringBuilder resultSB = new StringBuilder();
        resultSB.append(resultSet.getString(2));
        resultSB.append(",");
        resultSB.append(resultSet.getString(3));
        resultSB.append(",");
        resultSB.append(dateFormatToRedisFormat(resultSet.getString(4)));
        resultSB.append(",");
        resultSB.append(dateFormatToRedisFormat(resultSet.getString(5)));
        resultSB.append(",");
        resultSB.append(resultSet.getString(6));
        resultSB.append(",");
        resultSB.append(resultSet.getString(7));
        resultSB.append(",");
        resultSB.append(resultSet.getString(8));
        resultSB.append(",");
        resultSB.append(resultSet.getString(9));
        String ref1 = resultSet.getString(10);
        if (ref1 != null) {
          resultSB.append(",");
          resultSB.append(ref1);
        }
        keys.add(resultSB.toString());
      }
    } catch (SQLException e) {
      LOG.error("getRelatedKeys error", e);
    } finally {
      clearResultStatement(resultSet, statement);
    }
    return keys;
  }


}
