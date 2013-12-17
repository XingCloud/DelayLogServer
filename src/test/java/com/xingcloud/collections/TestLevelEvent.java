package com.xingcloud.collections;

import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 12/17/13
 * Time: 3:05 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestLevelEvent {
  @Test
  public void test(){
     String event="pay.*";
     LevelEvent levelEvent=new LevelEvent(event);
  }
}
