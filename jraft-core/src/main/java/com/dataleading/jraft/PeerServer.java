package com.dataleading.jraft;

import java.util.concurrent.atomic.AtomicLong;


public class PeerServer {

	private EndPoint endPoint;
	private volatile boolean voteGranted;

	//two volatile state on leaders, reinitialized after election
	private volatile long nextIndex=1;  //for each server, index of the next log entry to send to that server,
	                         //initialized to leader last log index + 1
	private volatile long matchIndex=0; //for each server,index of highest log entry known to be replicated on server,
	                         //initialized to 0, increases monotonically.
	
	private AtomicLong retries;

	public PeerServer(EndPoint endPoint) {
		this.endPoint = endPoint;
		retries = new AtomicLong(0);
	}

	public EndPoint getEndPoint() {
		return endPoint;
	}
	public long getNextIndex() {
		return nextIndex;
	}
	public void setNextIndex(long nextIndex) {
		this.nextIndex = nextIndex;
	}
	public long getMatchIndex() {
		return matchIndex;
	}
	public void setMatchIndex(long matchIndex) {
		this.matchIndex = matchIndex;
	}
	public boolean isVoteGranted() {
		return voteGranted;
	}
	public void setVoteGranted(boolean voteGranted) {
		this.voteGranted = voteGranted;
	}
	public AtomicLong getRtries() {
		return retries;
	}
	

}
