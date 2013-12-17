package com.xingcloud.collections;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 12/12/13
 * Time: 1:50 PM
 * To change this template use File | Settings | File Templates.
 */
public class LevelEvent {
  public String[] lEvents=new String[6];
  private int level;
  public LevelEvent(String event){
     String[] levelEvents=event.split(".");
     int i=levelEvents.length-1;
     while(levelEvents[i].equals("*"))
       i--;
     level=i;
     while(i>=0){
       lEvents[i]=levelEvents[i];
       i--;
     }
  }
  public boolean contains(LevelEvent event){
     for(int i=0;i<level;i++){
       if(!lEvents[i].equals(event.lEvents[i])&&!lEvents[i].equals("*"))
         return false;
     }
     return true;
  }
  @Override
  public String toString(){
    StringBuilder builder=new StringBuilder();
    for(int i=0;i<level-1;i++){
      builder.append(lEvents[i]+".");
    }
    builder.append(lEvents[level-1]);
    return builder.toString();
  }
  @Override
  public int hashCode(){
    return toString().hashCode();
  }
  @Override
  public boolean equals(Object o){
     if(this==o)
       return true;
     if(o instanceof LevelEvent){
       LevelEvent lEvent=(LevelEvent)o;
       if(level!=lEvent.level)
         return false;
       for(int i=0;i<level;i++)
       {
         if(!lEvents[i].equals(lEvent.lEvents[i]))
           return false;
       }
     }
     return true;
  }
}
