package com.xingcloud.delayserver;

import com.xingcloud.delayserver.redisutil.RedisShardedPoolResourceManager;
import com.xingcloud.delayserver.thrift.LogService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import redis.clients.jedis.ShardedJedis;

/**
 * User: IvyTang
 * Date: 13-1-8
 * Time: 下午5:14
 */
public class ProcessDelayLog {

    private static final Log LOG = LogFactory.getLog(ProcessDelayLog.class);


    public static void main(String[] args) throws TTransportException {
        TNonblockingServerSocket socket = new TNonblockingServerSocket(9090);

        DelayServiceHandler handler = new DelayServiceHandler();

        LogService.Processor<DelayServiceHandler> processor = new LogService.Processor<DelayServiceHandler>(handler);

        THsHaServer.Args serverArgs = new THsHaServer.Args(socket);
        serverArgs.processor(processor);
        serverArgs.protocolFactory(new TBinaryProtocol.Factory(true, true));
        TServer server = new THsHaServer(serverArgs);

        clearRedisSignal();

        LOG.info("server start...");
        server.serve();


    }


    private static void clearRedisSignal() {
        ShardedJedis shardedRedis = null;
        try {
            shardedRedis = RedisShardedPoolResourceManager.getInstance().getCache(0);
            shardedRedis.del("delaysignal");
            LOG.info("delete redis delay signal.");
        } catch (Exception e) {
            LOG.error(e.getMessage());
            RedisShardedPoolResourceManager.getInstance().returnBrokenResource(shardedRedis);
            shardedRedis = null;
        } finally {
            RedisShardedPoolResourceManager.getInstance().returnResource(shardedRedis);
        }
    }


}
