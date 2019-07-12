package com.dataleading.jraft;

import java.util.ArrayList;
import java.util.List;

/**
 * @author shshen
 *
 */
public class RaftLog {

	private List<LogEntry> logEntries;
	private long offset;  //the lastIndex of stateMachine
	private Object mutex = new Object();

	public RaftLog(long offset) {
		this.offset = offset;
		logEntries = new ArrayList<LogEntry>();
	}

	//invoked by clientService, the index start from 1.
	public long appendLogEntry(LogEntry entry) {
		synchronized (mutex) {
			long k = offset; 
			logEntries.add(entry);
			k = offset + logEntries.size();
			entry.setIndex(k);
			return k;			
		}
	}
	
	public void appendLogEntry(List<LogEntry> entries) {
		synchronized (mutex) {
			long k = offset;
			for(LogEntry e: entries) {
				logEntries.add(e);
				k = offset + logEntries.size();
				e.setIndex(k);
			}
		}
	}	

	/**
	 * delete the logIndex one, and all before it.
	 */
	public void deleteBefore(long logIndex) {
		synchronized (mutex) {
			if (logIndex >= 0 && logIndex < offset+logEntries.size()) {
				logEntries.subList(0, (int) (logIndex-offset)).clear();
				offset = logIndex;
			}
		}
	}
	/**
	 * delete the logIndex, and all following it.
	 */
	public void deleteAfter(long logIndex) {
		synchronized (mutex) {
			if (logIndex > 0  && logIndex <= offset+logEntries.size()) {
				logEntries.subList((int) (logIndex-offset), logEntries.size()).clear();;
				offset = logIndex;
			}			
		}
	}	

	public LogEntry getLogEntry(long logIndex) {
		synchronized (mutex) {
			LogEntry entry = null;
			int i = (int) (logIndex-offset)-1;
			if(i>=0 && i<logEntries.size()) {
				entry = logEntries.get(i);
			}
			return entry;
		}	
	}

	public long getLastLogIndex() {
		synchronized (mutex) {
			long idx  = 0;
			if(!logEntries.isEmpty()) {
				idx = logEntries.get(logEntries.size()-1).getIndex();
			}
			return idx;			
		}
	}
	
	public long getLastLogTerm() {
		synchronized (mutex) {
			long idx  = 0;
			if(!logEntries.isEmpty()) {
				idx = logEntries.get(logEntries.size()-1).getTerm();
			}
			return idx;			
		}
	}

	public long getFirstLogIndex() {
		synchronized (mutex) {
			long idx  = 0;
			if(!logEntries.isEmpty()) {
				idx = logEntries.get(0).getIndex();
			}
			return idx;	
		}
	}

	public void reset(long lastIndex) {
		// TODO Auto-generated method stub
		
	}

}
