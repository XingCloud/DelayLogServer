package com.xingcloud.readredis;

import com.xingcloud.collections.CacheKeyInfo;
import com.xingcloud.collections.FilterKey;
import com.xingcloud.collections.OrignalData;
import com.xingcloud.delayserver.redisutil.RedisShardedPoolResourceManager;
import com.xingcloud.delayserver.util.Constants;
import com.xingcloud.dumpredis.DumpRedis;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.ShardedJedis;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 12/16/13
 * Time: 4:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class ReadRedisKeyFile implements Runnable {
  public static Log LOG = LogFactory.getLog(ReadRedisKeyFile.class);

  private static String REDIS_KEYCACHE_FILE = Constants.REDIS_CACHE_DIR + Constants.KEY_CACAHE_FILE;

  public ReadRedisKeyFile() {

  }


  @Override
  public void run() {
    LOG.info("read redis key file thread start");
     while(true){

       try {
         Thread.sleep(20000);
       } catch (InterruptedException e) {
         e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
       }

       if(ifNeedReadRedisKeyFile()){
         try {
           readFile();
         } catch (Exception e) {
           e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
         }
       }
     }
  }

  private boolean ifNeedReadRedisKeyFile() {
    LOG.info("test if need read redis key file");
    ShardedJedis shardedRedis = null;
    try {
      shardedRedis = RedisShardedPoolResourceManager.getInstance().getCache(0);
      String result = shardedRedis.lpop(Constants.SIGNAL_READ);
      LOG.info("get result "+result);
      if (result != null && result.equals(Constants.SIGNAL_READY))
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

  public void readFile() throws Exception {
    LOG.info("start read redis key file");
    OrignalData.getInstance().clear();
    long t1=System.currentTimeMillis();
    try {

      File file = new File(REDIS_KEYCACHE_FILE);
      if (!file.exists()) {
        LOG.info(REDIS_KEYCACHE_FILE + "does not exist ");
        return;
      }
      BufferedReader reader = new BufferedReader(new FileReader(file));

      String line;
      String type, projectId, startDay, endDay, event, segment, ref0;
      int i=0;
      while ((line = reader.readLine()) != null&&line.length()>0) {
        String[] fields = line.split("\t");
        type = fields[1];
        projectId = fields[2];
        startDay = fields[3];
        endDay = fields[4];
        event = fields[5];
        segment = fields[6];
        ref0 = fields[7];
        String ref1 = null;
        if (fields.length > 8)
          ref1 = fields[8];
        FilterKey filterKey=new FilterKey(projectId,event);
        OrignalData.getInstance().addCacheKey(filterKey,
          new CacheKeyInfo(type,Long.valueOf(startDay),Long.valueOf(endDay),segment,ref0,ref1));

        i++;
      }

    } catch (FileNotFoundException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    LOG.info("finish read");
    DumpRedis.sendProcessSignalToRedis();
    LOG.info("read redis key file using "+(System.currentTimeMillis()-t1)+" ms");
    //To change body of implemented methods use File | Settings | File Templates.
  }

  public static void sendReadSigalToRedis() throws IOException, InterruptedException {

    DumpRedis.sendSignalToRedis(Constants.SIGNAL_READ,Constants.SIGNAL_READY);
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length != 0) {
      String cmd = args[0];
      if (cmd.equals("readRedisKey"))
        ReadRedisKeyFile.sendReadSigalToRedis();
    }
  }
}
