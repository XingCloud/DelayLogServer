package com.xingcloud.delayserver.util;

/**
 * User: IvyTang
 * Date: 13-1-8
 * Time: 下午6:13
 */
public class Constants {

    public static final String TIMEZONE = "GMT+8";

    public static final int EVENT_ITEMS_NUM = 5;

    public static final float PAY_ADCALC_DELAY_RATIO = 0f;

    public static final float DELAY_RATIO = 0.05f;

    public static final int SHARD_COUNT = 16;

    public static final String RDB = "dump.rdb";
    //public static final String RDB_SUFFIX = ".rdb";

    public static final String PARSEKEY = "parsekey";

    public static final String KEY_CACAHE_FILE = "keycache";

    public static final String FILTER_FILE = "filter";

    public static final int REDIS_CACHE_DB = 0;

    public static final int SCPPARSER_EXECUTOR_WORKTHREADS = 4;

    //public static String REDIS_CACHE_DIR = "/home/hadoop/ivytest/redis_cachedump_donotdelete/";
    public static final String REDIS_CACHE_DIR = "/data/redis_cachedump_donotdelete/";

    public static final String DUM_FILE_PREFIX = "/data/redis-dump/";

    public static final String[] REDIS_IPS = new String[]{"192.168.1.61"};

    public static final String SIGNAL_DUMP = "delaydump";

    public static final String SIGNAL_PROCESS= "delayprocess";

    public static final String SIGNAL_READY= "ready";
}
