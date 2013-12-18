package com.xingcloud.dumpredis;

import org.junit.Test;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 12/18/13
 * Time: 11:29 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestSendSignal {
  @Test
  public void sendDumpSignal() throws IOException, InterruptedException {
    DumpRedis.sendDumpSignalToRedis();
  }
  @Test
  public void sendProcessSignal() throws Exception {
    DumpRedis.sendProcessSignalToRedis();
  }
}
