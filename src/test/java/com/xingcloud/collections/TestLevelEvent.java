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
  public void testEventContain(){
     String event="visit.*",event1="*.auto.auto.*",event2="visit.auto.*",event3="visit.*.pay.*";
     LevelEvent levelEvent=new LevelEvent(event);
     LevelEvent levelEvent1=new LevelEvent(event1);
     System.out.println(levelEvent.contains(levelEvent1));

     LevelEvent levelEvent2=new LevelEvent(event2);
     System.out.println(levelEvent.contains(levelEvent2));

     LevelEvent levelEvent3=new LevelEvent(event3);
     System.out.println(levelEvent3.contains(levelEvent));
  }
}
