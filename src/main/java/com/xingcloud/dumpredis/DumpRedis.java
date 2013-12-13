package com.xingcloud.dumpredis;

import com.sun.org.apache.bcel.internal.classfile.ConstantString;
import com.xingcloud.collections.AnaResultTable;
import com.xingcloud.collections.FilterDelayEventRelationShip;
import com.xingcloud.collections.OrignalData;
import com.xingcloud.delayserver.redisutil.RedisShardedPoolResourceManager;
import com.xingcloud.delayserver.util.Constants;
import com.xingcloud.delayserver.util.Helper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;


import java.io.*;
import java.text.ParseException;
import java.util.*;

/**
 * User: IvyTang
 * Date: 13-1-5
 * Time: 下午2:06
 */
public class DumpRedis implements Runnable {

  private static Log LOG = LogFactory.getLog(DumpRedis.class);


  private static String REDIS_KEYCACHE_FILE = Constants.REDIS_CACHE_DIR + Constants.KEY_CACAHE_FILE;

  private static String REDIS_EVENTFILTER_FILE = Constants.REDIS_CACHE_DIR + Constants.FILTER_FILE;

  private static String MySQL_DBNAME = "redis_cachekey";

  private static String MYSQL_USER = "cachekey";

  private static String MySQL_PWD = "23XFdx5";

  private Map<String, Set<String>> eventFilters = new HashMap<String, Set<String>>();

  @Override
  public void run() {
     LOG.info("dumpredis start.....");
     while(true){
       try {
         Thread.sleep(1000000);
         if(ifNeedDumpDelayLog()){
           LOG.info("get ifNeedDumpDelayLog signal from redis");
           dump();
         }
       } catch (Exception e) {
         e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
       }
     }
  }

  public void dump() throws Exception {
    LOG.info("start to  dump data from redis and parse.");
    long currentTime = System.currentTimeMillis();
    OrignalData.getInstance().clear();
    FilterDelayEventRelationShip.getInstance().clear();
    ParseRDB parseRDB = new ParseRDB();
    parseRDB.scpFromRemoteAndParse();
    LOG.info("parse cache key to local file and load in Mem using " + (System.currentTimeMillis() - currentTime) + "ms.");
    //loadToMySQL();
  }


  private void sendDelaySigalToRedis() throws IOException, InterruptedException {

    ShardedJedis shardedRedis = null;
    try {
      shardedRedis = RedisShardedPoolResourceManager.getInstance().getCache(0);
      shardedRedis.del("delaysignal");
      shardedRedis.lpush("delaysignal", "dump");
      LOG.info("send delay dump sinal to redis...");
    } catch (Exception e) {
      LOG.error(e.getMessage());
      RedisShardedPoolResourceManager.getInstance().returnBrokenResource(shardedRedis);
      shardedRedis = null;
    } finally {
      RedisShardedPoolResourceManager.getInstance().returnResource(shardedRedis);
    }
  }

  private boolean ifNeedDumpDelayLog() {
    ShardedJedis shardedRedis = null;
    try {
      shardedRedis = RedisShardedPoolResourceManager.getInstance().getCache(0);
      String result = shardedRedis.lpop("delaysignal");
      if (result != null && result.equals("dump"))
        return true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      RedisShardedPoolResourceManager.getInstance().returnBrokenResource(shardedRedis);
      shardedRedis = null;
    } finally {
      RedisShardedPoolResourceManager.getInstance().returnResource(shardedRedis);
    }
    return false;
  }

  public static void main(String[] args) throws Exception {
    DumpRedis dumpRedis = new DumpRedis();
    if (args.length != 0) {
      String cmd = args[0];
      if (cmd.equals("processdelay"))
        dumpRedis.sendDelaySigalToRedis();
    }
  }


}
