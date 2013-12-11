package com.xingcloud.delayserver.test;

import com.xingcloud.delayserver.redisutil.RedisShardedPoolResourceManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;

import java.util.Collection;
import java.util.Map;

/**
 * User: IvyTang
 * Date: 13-3-12
 * Time: 下午3:31
 */
public class RedisTest {

    public static void main(String[] args) {
        ShardedJedis shardedRedis = null;
        try {
            shardedRedis = RedisShardedPoolResourceManager.getInstance().getCache(0);
            Collection<JedisShardInfo> infos = shardedRedis.getAllShardInfo();
            System.out.println(infos.size());
//            for (JedisShardInfo shardInfo : infos)
//                System.out.println(shardInfo.getHost() + shardInfo.getName() + shardInfo.getPort());
            Map<String,String> results = shardedRedis.hgetAll("COMMON,age,2013-02-14,2013-02-16,pay.*,TOTAL_USER,VF-ALL-0-0,PERIOD");
            for(Map.Entry<String,String> result:results.entrySet())
                System.out.println(result.getKey()+"\t"+result.getValue());

        } catch (Exception e) {

            RedisShardedPoolResourceManager.getInstance().returnBrokenResource(shardedRedis);
            shardedRedis = null;
        } finally {
            RedisShardedPoolResourceManager.getInstance().returnResource(shardedRedis);
        }
    }
}
