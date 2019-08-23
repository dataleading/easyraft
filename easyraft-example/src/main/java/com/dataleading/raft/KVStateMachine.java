package com.dataleading.raft;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.json.JSONObject;

public class KVStateMachine implements StateMachine {

  private long lastIndex;
  private long lastTerm;
  private Map<String, String> dataMap;
  
  public KVStateMachine() {
    dataMap = new HashMap<String, String>();
  }

  @Override
  public void writeSnapshot() {
    
  }

  @Override
  public Snapshot readSnapshot() {
    return null;
  }

  @Override
  public void loadSnapshot(Snapshot snapshot) {

  }

  @Override
  public void apply(LogEntry logEntry) {
    synchronized (this) {
      this.lastIndex = logEntry.getIndex();
      this.lastTerm = logEntry.getTerm();

      JSONObject data = logEntry.getData();

      Set<String> keys = data.keySet();
      for (String k : keys) {
        String v = data.getString(k);
        dataMap.put(k, v);
      }
    }
  }

  @Override
  public long getLastIndex() {
    return lastIndex;
  }

  @Override
  public long getLastTerm() {
    return lastTerm;
  }

  @Override
  public String query(String key) {
    return dataMap.get(key);
  }

}
