package com.xingcloud.dumpredis;

import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 12/18/13
 * Time: 11:00 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestDumpRedis {
  @Test
  public void testDumpRedis() throws InterruptedException {
    ExecutorService executorService=new ThreadPoolExecutor(1,2,3600, TimeUnit.SECONDS,new ArrayBlockingQueue<Runnable>(3));
    executorService.execute(new DumpRedis());
    executorService.shutdown();
    executorService.awaitTermination(3600,TimeUnit.SECONDS);
  }
}
