package com.xingcloud.dumpredis;

import com.xingcloud.delayserver.redisutil.RedisShardedPoolResourceManager;
import com.xingcloud.delayserver.util.Constants;
import com.xingcloud.delayserver.util.Helper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import com.xingcloud.collections.*;
import redis.clients.jedis.ShardedJedis;

/**
 * User: IvyTang
 * Date: 13-3-4
 * Time: 下午4:16
 */
public class ParseRDB {

    private static final Log LOG = LogFactory.getLog(ParseRDB.class);

    public boolean scpFromRemoteAndParse() throws InterruptedException {

        clearCacheDir();

        //支持多台redis的多个shard一起分析
        ScpParseExecutor executor = new ScpParseExecutor();
        for (String redisIP : Constants.REDIS_IPS) {
            for (int i = 1; i <= Constants.SHARD_COUNT; i++) {

                String remoteDumpFile = Constants.DUM_FILE_PREFIX + "s" + i + File.separator + Constants.RDB;

                String localDumpFile = Constants.REDIS_CACHE_DIR + Constants.RDB + "_" + redisIP + "_" + i;
                String localParseFile = Constants.REDIS_CACHE_DIR + Constants.PARSEKEY + "_" + redisIP + "_" + i;
                String localParseKeyFile = Constants.REDIS_CACHE_DIR + Constants.KEY_CACAHE_FILE + "_" + redisIP + "_" + i;
                String localParseFilterFile = Constants.REDIS_CACHE_DIR + Constants.FILTER_FILE + "_" + redisIP + "_" + i;
                ScpParseChildThread childThread = new ScpParseChildThread(redisIP, remoteDumpFile, localDumpFile,
                        localParseFile, localParseKeyFile, localParseFilterFile);
                executor.execute(childThread);
            }
        }
        executor.shutdown();
        boolean result = executor.awaitTermination(2, TimeUnit.HOURS);
        if (!result)
            return result;
        ShardedJedis shardedRedis = RedisShardedPoolResourceManager.getInstance().getCache(0);
        shardedRedis.del(Constants.SIGNAL_PROCESS);
        shardedRedis.lpush(Constants.SIGNAL_PROCESS,Constants.SIGNAL_READY);
        LOG.info("scp from remote and parse finished . send signal "+Constants.SIGNAL_PROCESS);
        return true;
    }

    private void clearCacheDir() {
        LOG.info("Clear cache dir");
        File dir = new File(Constants.REDIS_CACHE_DIR);
        if (dir.exists() && dir.isDirectory()) {
            File delFiles[] = dir.listFiles();
            for (File delFile : delFiles)
                if (delFile.isFile()){
                    delFile.delete();
                    LOG.info("delete file "+delFile);
                }
        }
    }
}

class ScpParseChildThread implements Runnable {

    private static final Log LOG = LogFactory.getLog(ScpParseChildThread.class);

    private Map<String, Set<String>> filters = new HashMap<String, Set<String>>();

    private long MONTH_TIMEMILLIS = 30 * 24 * 3600 * 1000l;

    private String VF_ALL = "VF-ALL-0-0";


    private String remoteRedis;
    private String remoteDumpFile;
    private String localDumpFile;
    private String localParseFile;
    private String localParseKeyFile;
    private String localParseFilterFile;


    public ScpParseChildThread(String remoteRedis, String remoteDumpFile, String localDumpFile, String localParseFile,
                               String localParseKeyFile, String localParseFilterFile) {
        this.remoteRedis = remoteRedis;
        this.remoteDumpFile = remoteDumpFile;
        this.localDumpFile = localDumpFile;
        this.localParseFile = localParseFile;
        this.localParseKeyFile = localParseKeyFile;
        this.localParseFilterFile = localParseFilterFile;
    }

    @Override
    public void run() {

        long currentTime = System.currentTimeMillis();

        //scp 远程的rdb文件到本地
        String scpCmd = "scp " + remoteRedis + ":" + remoteDumpFile + " " + localDumpFile;
        Helper.execShell(scpCmd);
        LOG.info(remoteRedis + "scp to local " + scpCmd + " using " + (System.currentTimeMillis() - currentTime) + "ms.");

        //把rdb文件用rdb tools转成json格式
        currentTime = System.currentTimeMillis();
        String parseCmd = "rdb --command json --db " + Constants.REDIS_CACHE_DB + " " + localDumpFile + " > " + localParseFile;
        Helper.execShell(parseCmd);
        LOG.info(remoteRedis + "parsetojson " + parseCmd + " using " + (System.currentTimeMillis() - currentTime) + "ms.");

        //把json格式变为mysql load需要的数据格式
        currentTime = System.currentTimeMillis();
        parseToMem();
        LOG.info(remoteRedis + "parseToMem using " + (System.currentTimeMillis() - currentTime) + "ms.");

    }

    private void parseToMem(){
      LOG.info("begin to parse to Mem...");
      BufferedReader reader = null;
      BufferedWriter keyCacheWriter = null;
      BufferedWriter filterWriter = null;
      try {
        reader = new BufferedReader(new FileReader(localParseFile));
        String currentLine = null;
        String lastLine = null;

        //读rdb的生成的json文件，第一行舍弃，最后一行去掉不用的{}
        while (true) {
          lastLine = currentLine;
          currentLine = reader.readLine();
          if (lastLine == null) { //第一行
            currentLine = reader.readLine();
            lastLine = currentLine;
          }
          if (currentLine == null) {//最后一行
            if (lastLine != null) {
              parseOneLineToMem(lastLine, true);
            }
            break;
          } else {
            parseOneLineToMem(lastLine, false);
          }
        }
      } catch (FileNotFoundException e) {
        LOG.error("FileNotFoundException", e);
      } catch (IOException e) {
        LOG.error("IOException", e);
      } finally {
        try {
          if (reader != null) {
            reader.close();
          }
        } catch (IOException e) {
          LOG.error("IOException", e);
        }
      }

    }

    private void parseOneLineToMem(String line, boolean ifLastLine){
      String lineBack = line;
      if (!line.contains(VF_ALL))
          return ;
      try {
        if (ifLastLine) {
          line = line.substring(0, line.length() - 15);
        } else {
          line = line.substring(0, line.length() - 1);
        }
        int keyValueSep = line.lastIndexOf("{");
        String key = line.substring(0, keyValueSep - 1);
        key = key.substring(1, key.length() - 1);


        //前五项 COMMON/GROUPBY ， pid ,startdate, enddate,eventfilter, 可以用，找到

        //type: COMMON/GROUP
        int commaIndex = key.indexOf(",");
        String type = key.substring(0, commaIndex);
        key = key.substring(commaIndex + 1, key.length());

        //pid
        commaIndex = key.indexOf(",");
        String pid = key.substring(0, commaIndex);
        key = key.substring(commaIndex + 1, key.length());

        //startdate and enddate
        String[] dates = new String[2];
        for (int i = 0; i < 2; i++) {
          commaIndex = key.indexOf(",");
          dates[i] = key.substring(0, commaIndex).replaceAll("-", "");
          key = key.substring(commaIndex + 1, key.length());
        }
        //如果enddate在30天内，这条cache合法
        if (!checkEventDate(dates[1], System.currentTimeMillis()))
          return ;

        //eventfilter
        commaIndex = key.indexOf(",");
        String event = key.substring(0, commaIndex);
        key = key.substring(commaIndex + 1, key.length());

        //eventfilter如果不合法，返回
        if (!checkEventFilterLegal(event))
          return ;

        //处理eventfilter
        Set<String> pFilters = filters.get(pid);
        if (pFilters == null) {
          pFilters = new HashSet<String>();
          filters.put(pid, pFilters);
        }
        pFilters.add(event);


        //切分出segment
        int segmentLastIndex = key.indexOf(VF_ALL) - 1;
        String segment=key.substring(0, segmentLastIndex);

        //剩下的内容为period
        key = key.substring(segmentLastIndex + VF_ALL.length() + 2);
        int comma = -1;
        String timeUnitType,ref;
        comma=key.indexOf(",");
        if(comma>0){
          timeUnitType=key.substring(0,comma);
          ref=key.substring(comma+1,key.length());
        }else {
          timeUnitType=key;
          ref=null;
        }

        FilterKey filterKey=new FilterKey(pid,event);
        CacheKeyInfo cacheKeyInfo=new CacheKeyInfo(type,Long.valueOf(Helper.getDate(Long.valueOf(dates[0]))),
          Long.valueOf(Helper.getDate(Long.valueOf(dates[1]))),segment,timeUnitType,ref);
        OrignalData.getInstance().addCacheKey(filterKey,cacheKeyInfo);

      } catch (Exception e) {
        LOG.warn("parse one line to mysql error." + lineBack, e);
      }

    }

    private boolean checkEventFilterLegal(String event) {
        String[] tmps = event.split("\\.");
        return tmps.length == 6 || event.endsWith("*");

    }

    private boolean checkEventDate(String endDate, long currentTime) throws ParseException {
        return (currentTime - MONTH_TIMEMILLIS) <= Helper.getTimestamp(endDate);
    }

}
