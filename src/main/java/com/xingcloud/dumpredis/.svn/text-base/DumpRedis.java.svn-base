package com.xingcloud.dumpredis;

import com.sun.org.apache.bcel.internal.classfile.ConstantString;
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
public class DumpRedis {

    private static Log LOG = LogFactory.getLog(DumpRedis.class);


    private static String REDIS_KEYCACHE_FILE = Constants.REDIS_CACHE_DIR + Constants.KEY_CACAHE_FILE;

    private static String REDIS_EVENTFILTER_FILE = Constants.REDIS_CACHE_DIR + Constants.FILTER_FILE;

    private static String MySQL_DBNAME = "redis_cachekey";

    private static String MYSQL_USER = "cachekey";

    private static String MySQL_PWD = "23XFdx5";

    private Map<String, Set<String>> eventFilters = new HashMap<String, Set<String>>();


    public void dump() throws Exception {
        long currentTime = System.currentTimeMillis();
        ParseRDB parseRDB = new ParseRDB();
        parseRDB.scpFromRemoteAndParse();
        LOG.info("parse key to local file using " + (System.currentTimeMillis() - currentTime) + "ms.");
        loadToMySQL();
    }


    public void loadToMySQL() throws IOException, InterruptedException {
        String cacheSql = String.format("use %s ;DELETE FROM %s ;LOAD DATA LOCAL INFILE '%s' INTO TABLE %s ;",
                MySQL_DBNAME, "cache", REDIS_KEYCACHE_FILE, "cache");
        String filterSql = String.format("use %s ;DELETE FROM %s ;LOAD DATA LOCAL INFILE '%s' INTO TABLE %s ;",
                MySQL_DBNAME, "filter", REDIS_EVENTFILTER_FILE, "filter");
        loadToTable(cacheSql);
        loadToTable(filterSql);

    }

    private void loadToTable(String sqlCmd) throws IOException, InterruptedException {
        Runtime rt = Runtime.getRuntime();
        String cmd = String.format("mysql -h%s -u%s -p%s -e\"%s\"", "192.168.1.134", MYSQL_USER,
                MySQL_PWD, sqlCmd);
        LOG.info(cmd);
        long currentTime = System.currentTimeMillis();
        String[] cmds = new String[]{"/bin/sh", "-c", cmd};
        int result = execShellCmd(rt, cmds);
        if (result != 0) {
            LOG.error("localToMySQL error." + sqlCmd);
        }
        LOG.info("loadToMySQL using " + (System.currentTimeMillis() - currentTime) + "ms.");
    }

    private int execShellCmd(Runtime rt, String[] cmds) throws IOException, InterruptedException {
        Process process = rt.exec(cmds);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String cmdOutput = null;
        while ((cmdOutput = bufferedReader.readLine()) != null)
            LOG.info(cmdOutput);
        return process.waitFor();
    }


    private void sendDelaySigalToRedis() throws IOException, InterruptedException {

        String clearSql = String.format("use %s ;DELETE FROM %s;DELETE FROM %s;", MySQL_DBNAME, "delayevent",
                "relationship");

        loadToTable(clearSql);

        ShardedJedis shardedRedis = null;
        try {
            shardedRedis = RedisShardedPoolResourceManager.getInstance().getCache(0);
            shardedRedis.lpush("delaysignal", "process");
            LOG.info("send delay process sinal to redis...");
        } catch (Exception e) {
            LOG.error(e.getMessage());
            RedisShardedPoolResourceManager.getInstance().returnBrokenResource(shardedRedis);
            shardedRedis = null;
        } finally {
            RedisShardedPoolResourceManager.getInstance().returnResource(shardedRedis);
        }
    }

    public static void main(String[] args) throws Exception {
        DumpRedis dumpRedis = new DumpRedis();
        if (args.length != 0) {
            String cmd = args[0];
            if (cmd.equals("dumptomysql")) {
                dumpRedis.dump();
            } else if (cmd.equals("processdelay"))
                dumpRedis.sendDelaySigalToRedis();
        }
    }
}
