package com.dataleading.raft;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;


public class RaftMessage {
	
	public static abstract class Message{
		private long term;
		public long getTerm() {
			return term;
		}
		public void setTerm(long term) {
			this.term = term;
		}

		public abstract String toJsonText();
		
		public String getPath() {
			return "/raft";
		}
	}

	public static class VoteRequest extends Message{
		
		private EndPoint candidateId;
		private long lastLogIndex;
		private long lastLogTerm;
		
		public VoteRequest(JSONObject obj) {
			setTerm(obj.getLong("term"));
			this.candidateId = new EndPoint(obj.getString("hostname"), obj.getInt("port"));
			this.lastLogIndex = obj.getLong("lastLogIndex");
			this.lastLogTerm = obj.getLong("lastLogTerm");
		}
		
		public VoteRequest(long term, EndPoint candidateId, long lastLogIndex, long lastLogTerm) {
			setTerm(term);
			this.candidateId = candidateId;
			this.lastLogIndex = lastLogIndex;
			this.lastLogTerm = lastLogTerm;
		}
		
		@Override
		public String toJsonText() {
			JSONObject obj = new JSONObject();
			obj.put("term", getTerm());
			obj.put("hostname", candidateId.getHostname());
			obj.put("port", candidateId.getPort());
			obj.put("lastLogIndex", lastLogIndex);
			obj.put("lastLogTerm", lastLogTerm);
			return obj.toString(0);
		}
		
		public EndPoint getCandidateId() {
			return candidateId;
		}
		public long getLastLogIndex() {
			return lastLogIndex;
		}
		public long getLastLogTerm() {
			return lastLogTerm;
		}

		@Override
		public String getPath() {
			return "/raft/voterequest";
		}
		
	}
	
	public static class VoteResponse extends Message{
		private boolean voteGranted;
		
		public VoteResponse(String rsp) {
			JSONObject obj = new JSONObject(rsp);
			setTerm(obj.getLong("term"));
			this.voteGranted = obj.getBoolean("voteGranted");
		}
		public VoteResponse(long term, boolean voteGranted) {
			setTerm(term);
			this.voteGranted = voteGranted;
		}
		
		@Override
		public String toJsonText() {
			JSONObject obj = new JSONObject();
			obj.put("term", getTerm());
			obj.put("voteGranted", voteGranted);
			return obj.toString(0);
		}
		public boolean getVoteGranted() {
			return voteGranted;
		}
	}

	public static class AppendEntriesRequest extends Message{
		
		private EndPoint serverId; //leaderId
		private long prevLogIndex;
		private long prevLogTerm;
		private List<LogEntry> logEntries;
		private long leaderCommit;
		
		public AppendEntriesRequest(JSONObject obj) {
			setTerm(obj.getLong("term"));
			this.serverId = new EndPoint(obj.getString("hostname"), obj.getInt("port"));
			this.prevLogIndex = obj.getLong("prevLogIndex");
			this.prevLogTerm = obj.getLong("prevLogTerm");
			logEntries = new ArrayList<LogEntry>();
			JSONArray array = obj.getJSONArray("logEntries");
			for(int i=0; i<array.length(); i++) {
				JSONObject o = array.getJSONObject(i);
				LogEntry le = new LogEntry(o.getLong("term"), o.getJSONObject("record"));
				long idx = o.getLong("index");
				le.setIndex(idx);
				logEntries.add(le);
			}
			this.leaderCommit = obj.getLong("leaderCommit");
		}
		
		public AppendEntriesRequest(long term, EndPoint serverId, long prevLogIndex, long prevLogTerm,
				List<LogEntry> logEntries, long leaderCommit) {
			this.setTerm(term);
			this.serverId = serverId;
			this.prevLogIndex = prevLogIndex;
			this.prevLogTerm = prevLogTerm;
			this.logEntries = logEntries;
			this.leaderCommit = leaderCommit;
		}

		@Override
		public String toJsonText() {
			JSONObject obj = new JSONObject();
			obj.put("term", getTerm());
			obj.put("hostname", serverId.getHostname());
			obj.put("port", serverId.getPort());
			obj.put("prevLogIndex", prevLogIndex);
			obj.put("prevLogTerm", prevLogTerm);

			JSONArray array = new JSONArray();
			for(LogEntry le: logEntries) {
				array.put(le.toJson());
			}
			obj.put("logEntries", array);
			obj.put("leaderCommit", leaderCommit);
			return obj.toString(0);
		}

		@Override
		public String getPath() {
			return "/raft/appendentries";
		}
		public EndPoint getServerId() {
			return serverId;
		}
		public long getPrevLogIndex() {
			return prevLogIndex;
		}	
		public long getPrevLogTerm() {
			return prevLogTerm;
		}
		public List<LogEntry> getLogEntries(){
			return logEntries;
		}
		public long getLeaderCommit() {
			return leaderCommit;
		}
		
	}

	public static class AppendEntriesResponse extends Message{
		
		private boolean success;

		public AppendEntriesResponse(String rsp) {
			JSONObject obj = new JSONObject(rsp);
			setTerm(obj.getLong("term"));
			this.success = obj.getBoolean("success");
		}
		
		public AppendEntriesResponse(long term, boolean success) {
			this.setTerm(term);
			this.success = success;
		}

		@Override
		public String toJsonText() {
			JSONObject obj = new JSONObject();
			obj.put("term", getTerm());
			obj.put("success", success);
			return obj.toString(0);
		}
		
		public boolean isSuccess() {
			return success;
		}

		public void setSuccess(boolean success) {
			this.success = success;
		}
	}
	
	public static class InstallSnapshotRequest extends Message{
		
		private EndPoint leaderId;
		private long lastIncludedIndex;
		private long lastIncludedTerm;
		private long offset;
		private byte[] data; 
		private boolean done = true; //true if this is the last chunk
		
		public InstallSnapshotRequest(JSONObject obj) {
			setTerm(obj.getLong("term"));
			this.leaderId = new EndPoint(obj.getString("hostname"), obj.getInt("port"));
			this.lastIncludedIndex = obj.getLong("lastIncludedIndex");
			this.lastIncludedTerm = obj.getLong("lastIncludedTerm");
			this.offset = obj.getLong("offset");
			this.data = Base64.getDecoder().decode(obj.getString("data"));
		}
		
		public InstallSnapshotRequest(long term, EndPoint leaderId, long lastIncludedIndex, long lastIncludedTerm, long offset, byte[] entries) {
			this.setTerm(term);
			this.leaderId = leaderId;
			this.lastIncludedIndex  = lastIncludedIndex;
			this.lastIncludedTerm = lastIncludedTerm;
			this.offset = offset;
			this.data = entries;
		}

		@Override
		public String toJsonText() {
			JSONObject obj = new JSONObject();
			obj.put("term", getTerm());
			obj.put("hostname", leaderId.getHostname());
			obj.put("port", leaderId.getPort());
			obj.put("lastIncludedIndex", lastIncludedIndex);
			obj.put("lastIncludedTerm", lastIncludedTerm);
			obj.put("offset", offset);
			obj.put("data", Base64.getEncoder().encodeToString(data));
			obj.put("done", done);
			return obj.toString(0);
		}
		
		public EndPoint getServerId() {
			return leaderId;
		}
		public long getLastIncludedIndex() {
			return lastIncludedIndex;
		}
		public long getLastIncludedTerm() {
			return lastIncludedTerm;
		}
		public byte[] getData() {
			return data;
		}
		
	}
	
	//currentTerm, for leader to update itself
	public static class InstallSnapshotResponse extends Message {
		
		public InstallSnapshotResponse(String rsp) {
			JSONObject obj = new JSONObject(rsp);
			setTerm(obj.getLong("term"));
		}
		public InstallSnapshotResponse(long term) {
			setTerm(term);
		}	
		@Override
		public String toJsonText() {
			JSONObject obj = new JSONObject();
			obj.put("term", getTerm());
			return obj.toString(0);
		}
	}
}
