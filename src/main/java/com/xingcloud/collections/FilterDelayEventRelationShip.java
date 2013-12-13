package com.xingcloud.collections;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 12/12/13
 * Time: 2:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class FilterDelayEventRelationShip {
  public Map<String,Map<String,List<FilterKey>>> relationShip;
  public static FilterDelayEventRelationShip instance=null;
  public static FilterDelayEventRelationShip getInstance(){
    if(instance==null)
      instance=new FilterDelayEventRelationShip();
    return instance;
  }
  private FilterDelayEventRelationShip(){
    relationShip=new HashMap<String, Map<String, List<FilterKey>>>();
  }
  public void clear(){
    relationShip.clear();
  }
  public void addRelation(String pid,String event,FilterKey filterKey){
    Map<String,List<FilterKey>> eventFiltersMap=relationShip.get(pid);
    if(eventFiltersMap==null)
    {
      eventFiltersMap=new HashMap<String, List<FilterKey>>();
      relationShip.put(pid,eventFiltersMap);
    }
    List<FilterKey> filterKeys=eventFiltersMap.get(event);
    if(filterKeys==null){
      filterKeys=new ArrayList<FilterKey>();
      eventFiltersMap.put(event,filterKeys);
    }
    filterKeys.add(filterKey);
  }
}
