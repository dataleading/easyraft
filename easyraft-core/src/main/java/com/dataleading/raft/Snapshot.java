package com.dataleading.raft;


public class Snapshot {
	
	private long lastIndex;
	private long lastTerm;

	public Snapshot(String file) {
		
	}
	
	public Snapshot(byte[] contents) {
		
	}
	
	public long getLastIndex() {
		return lastIndex;
	}
	
	public long getLastTerm() {
		return lastTerm;
	}
	
	public byte[] getContents() {
		return null;
	}
	
	
}
