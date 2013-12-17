package com.xingcloud.delayserver;

import com.xingcloud.collections.LevelEvent;
import com.xingcloud.delayserver.util.Constants;
import com.xingcloud.delayserver.util.Helper;
import com.xingcloud.delayserver.thrift.LogService;
import com.xingcloud.delayserver.redisutil.RedisShardedPoolResourceManager;
import com.xingcloud.dumpredis.DumpRedis;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import redis.clients.jedis.ShardedJedis;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: IvyTang
 * Date: 13-1-8
 * Time: 下午6:05
 */
public class DelayServiceHandler implements LogService.Iface {

    private static Log LOG = LogFactory.getLog(DelayServiceHandler.class);

    private long MONTH_TIMEMILLIS = 30 * 24 * 3600 * 1000l;

    //格式为<pid,<event,<date,<uid,value>>>>
    private Map<String, Map<String, Map<Long, Set<UidValue>>>> delayLogs;
    private Map<String, Set<String>> delayEvents;

    private AtomicInteger i = new AtomicInteger(0);

    private Set<String> blackDelayPids;


    public DelayServiceHandler() {
        LOG.info("DelayServiceHandler start.");
        delayLogs = new HashMap<String, Map<String, Map<Long, Set<UidValue>>>>();
        delayEvents = new HashMap<String, Set<String>>();
        blackDelayPids = new HashSet<String>();
        blackDelayPids.add("fishman");
        blackDelayPids.add("db-monitor");
        blackDelayPids.add("defender");
        Thread dumpThread=new Thread(new DumpRedis());
        dumpThread.start();
    }


    @Override
    public int send(List<String> logs) throws TException {
        return 0;
    }

    @Override
    public int sendLogDay(List<String> logs, long daytptime) throws TException {
        synchronized (this) {
            LOG.info("receive logs");
            processLogs(logs);
            int times = i.incrementAndGet();
            LOG.info("times is "+times);
            if (times > 100) {
                if (ifNeedAnalysisDelayLog()) {
                    LOG.info("get needAnalysisDelayLog signal from redis.");
                    Thread analysisThread = new Thread(new DelayAnalysisLogicRunnable(delayLogs, delayEvents));
                    analysisThread.start();
                    delayLogs = new HashMap<String, Map<String, Map<Long, Set<UidValue>>>>();
                    delayEvents = new HashMap<String, Set<String>>();
                }
                i.set(0);
            }
        }
        return 0;
    }

    //把每次读到的延迟log按项目/事件/日期的顺序放进map
    private void processLogs(List<String> logs) {
        //LOG.info("enter process Log");
        long currentTime = System.currentTimeMillis();
        for (String log : logs) {
            String[] tmps = log.split("\t");
            if (tmps.length != Constants.EVENT_ITEMS_NUM) {
                LOG.warn(log);
                continue;
            }

            if (blackDelayPids.contains(tmps[0]))
                continue;

            //log的发生时间在1个月前，忽略过长时间之前的延迟log
            if ((currentTime - MONTH_TIMEMILLIS) > Long.valueOf(tmps[4]))
                continue;

            //每个项目对应的Map的value
            Map<String, Map<Long, Set<UidValue>>> pDelayLogs = delayLogs.get(tmps[0]);
            if (pDelayLogs == null) {
                pDelayLogs = new HashMap<String, Map<Long, Set<UidValue>>>();
                delayLogs.put(tmps[0], pDelayLogs);
            }

            //每个事件对应的Map的value
            Map<Long, Set<UidValue>> pEventDelayLogs = pDelayLogs.get(tmps[2]);
            if (pEventDelayLogs == null) {
                pEventDelayLogs = new HashMap<Long, Set<UidValue>>();
                pDelayLogs.put(tmps[2], pEventDelayLogs);
            }

            //事件的发生时间对应的Map的value
            long eventDate = Long.valueOf(Helper.getDate(Long.valueOf(tmps[4])));
            Set<UidValue> personDelayLogs = pEventDelayLogs.get(eventDate);
            if (personDelayLogs == null) {
                personDelayLogs = new HashSet<UidValue>();
                pEventDelayLogs.put(eventDate, personDelayLogs);
            }
            UidValue uidValue = new UidValue(Integer.valueOf(tmps[1]), Long.valueOf(tmps[3]),
                    Long.valueOf(tmps[4]));
            //UidValue判断相同的标准是uid和ts相同，如果personDelayLogs用这个uidvalue，说明这个uidvalue需要被覆盖；
            //则删除之前的uidvalue，加入新的uidvalue
//            if (personDelayLogs.contains(uidValue)) {
            personDelayLogs.remove(uidValue);
//            }
            personDelayLogs.add(uidValue);
            Set<String> events = delayEvents.get(tmps[0]);
            if (events == null) {
                events = new HashSet<String>();
                delayEvents.put(tmps[0], events);
            }
            events.add(tmps[2]);
        }
     // LOG.info("leave process LOG");
    }

    private boolean ifNeedAnalysisDelayLog() {
        LOG.info("test if need analysis delay log");
        ShardedJedis shardedRedis = null;
        try {
            shardedRedis = RedisShardedPoolResourceManager.getInstance().getCache(0);
            String result = shardedRedis.lpop(Constants.SIGNAL_PROCESS);
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



}
