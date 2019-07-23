package com.dataleading.raft;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONObject;
import com.dataleading.raft.RaftMessage.AppendEntriesRequest;
import com.dataleading.raft.RaftMessage.AppendEntriesResponse;
import com.dataleading.raft.RaftMessage.InstallSnapshotRequest;
import com.dataleading.raft.RaftMessage.InstallSnapshotResponse;
import com.dataleading.raft.RaftMessage.Message;
import com.dataleading.raft.RaftMessage.VoteRequest;
import com.dataleading.raft.RaftMessage.VoteResponse;

/**
 * https://raft.github.io/
 * http://openlife.cc/system/files/4-modifications-for-Raft-consensus.pdf
 * 
 * @author shshen
 *
 */
public class RaftNode {

	private static Logger logger = Logger.getLogger(RaftNode.class.getName());

	public enum NodeState {
		FOLLOWER, CANDIDATE, LEADER
	}

	private NodeState state = NodeState.FOLLOWER;
	private EndPoint leaderId;
	private EndPoint serverId;
	private EndPoint votedFor = null; // candidateId that received vote in current term (or null if none)

	private long currentTerm = 0; // latest term server has been

	private RaftLog raftLog; // log entries; each entry contains command for state machine, and term when
	                                   // entry was received by leader (first index is 1)

	private volatile long commitIndex = 0; // index of highest log entry known to be committed
	private volatile long lastAppliedIndex = 0; // index of highest log entry applied to state machine.

	private Set<PeerServer> peerServers;
	private StateMachine stateMachine;

	private ExecutorService executorService;
	private ScheduledThreadPoolExecutor scheduledExecutorService;
	private ScheduledFuture<?> scheduledElection;
	private ScheduledFuture<?> scheduledHeartbeat;

	private final int snapshotIntervalSeconds = 3600;
	private final int heartbeatIntervalMilliseconds = 1000;
	private final int electionTimeoutMilliseconds = 5000;
	private final int maxLogEntriesPerRequest = 100;
	
	private Object commitMutex =  new Object();
	private boolean asyncWrite = false;

	public RaftNode(Set<PeerServer> peerServers, EndPoint serverId, StateMachine stateMachine) {
		this.stateMachine = stateMachine;
		raftLog = new RaftLog(stateMachine.getLastIndex()); 
		
		this.peerServers = peerServers;
		this.serverId = serverId;
		
		// init thread pool
		executorService = new ThreadPoolExecutor(16, 16, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());

		scheduledExecutorService = new ScheduledThreadPoolExecutor(3);
		scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				takeSnapshot();
			}
		}, snapshotIntervalSeconds, snapshotIntervalSeconds, TimeUnit.SECONDS);

		// start election
		restartElectionTimer();
	}

	private void restartElectionTimer() {
		if (scheduledElection != null && !scheduledElection.isDone()) {
			scheduledElection.cancel(true);
		}
		
		for(PeerServer peer: peerServers) {
			peer.setVoteGranted(false);
		}
		int randomElectionTimeout = electionTimeoutMilliseconds
				+ ThreadLocalRandom.current().nextInt(electionTimeoutMilliseconds);		

		logger.log(Level.FINE, "new election time is after {0} ms", randomElectionTimeout+"");
		scheduledElection = scheduledExecutorService.schedule(new Runnable() {
			@Override
			public void run() {
				if (state == NodeState.LEADER) {
					logger.log(Level.SEVERE, "during leader election timer, this should not happen!");
					System.exit(-1);
				}

				currentTerm = currentTerm + 1;
				votedFor = serverId;
				state = NodeState.CANDIDATE;

				requestVote();
				if(state != NodeState.LEADER) {
					restartElectionTimer();					
				}
			}
		}, randomElectionTimeout, TimeUnit.MILLISECONDS);
	}

	private void requestVote() {
		for (final PeerServer peer : peerServers) {
			peer.setVoteGranted(false);
		}
		
		List<Future<?>> tasks =  new ArrayList<Future<?>>();
		
		for (final PeerServer peer : peerServers) {
			Future<?> f = executorService.submit(new Runnable() {
				@Override
				public void run() {
					VoteRequest request = new VoteRequest(currentTerm, serverId, raftLog.getLastLogIndex(),
							raftLog.getLastLogTerm());
					
					String o = postRequest(peer, request);				
					VoteResponse resp = null;
					try {
						resp = new VoteResponse(o);
					} catch (Exception e) {
						logger.log(Level.WARNING, "vote error resp from {0},json=''{1}''", new Object[]{peer.getEndPoint(), o});	
						return;
					}
					
					if (currentTerm != request.getTerm() || state != NodeState.CANDIDATE) {
						logger.log(Level.INFO, "node term or state has changed,ignore the vote.term={0},state={1}", new Object[]{currentTerm+"", state.toString()});
						return;
					}
					
					peer.setVoteGranted(resp.getVoteGranted());					
					if(resp.getTerm() > currentTerm) {
						logger.log(Level.INFO, "vote denied by {0} with term {1}, my term is {2}, become follower.",
								new Object[] { peer.getEndPoint(), resp.getTerm(), currentTerm });
						becomeFollower(resp.getTerm());
					}else {
						if(resp.getVoteGranted()) {
							logger.log(Level.INFO, "vote granted by {0} with term {1}, my term is {1}",
									new Object[] { peer.getEndPoint(), resp.getTerm(), currentTerm });
							peer.setVoteGranted(true);
						}else {
							logger.log(Level.INFO, "vote denied by {0} with term {1}, my term is {2}",
									new Object[] { peer.getEndPoint(), resp.getTerm(), currentTerm });
						}
						int voteGrantedNum = 0;
						for(PeerServer p: peerServers) {
							if(p.isVoteGranted()) {
								voteGrantedNum = voteGrantedNum + 1;
							}
						}
						if(votedFor.equals(serverId)) {
							voteGrantedNum = voteGrantedNum + 1;
						}
						if(voteGrantedNum > (peerServers.size()+1)/2) {
							becomeLeader();								
						}
					}
				}
			});		
			tasks.add(f);
		}
		timeoutTasks(tasks);
	}

	private void timeoutTasks(List<Future<?>> tasks) {
		//timeout the vote requests.
		TimeUnit unit =  TimeUnit.MILLISECONDS;
		long nanos = unit.toNanos(100);
		final long deadline = System.nanoTime() + nanos;
		boolean done = false;
        try {
			for (int i = 0; i < 2; i++) {
			    Future<?> ff = tasks.get(i);
			    if (!ff.isDone()) {
			        if (nanos <= 0L)
			            return;
			        try {
			            ff.get(nanos, TimeUnit.NANOSECONDS);
			        } catch (CancellationException ignore) {
			        } catch (ExecutionException ignore) {
			        } catch (TimeoutException toe) {
			            return;
			        } catch (InterruptedException e) {
					}
			        nanos = deadline - System.nanoTime();
			    }
			}
			done = true;
		} finally {
            if (!done)
                for (int i = 0, size = tasks.size(); i < size; i++)
                    tasks.get(i).cancel(true);
		}		
	}

	private synchronized void becomeLeader() {
		if (state != NodeState.LEADER) {			
			logger.log(Level.INFO, "got majority votes, this server {0} become leader",
					new Object[] { serverId });
			
			// stop election timer
			if (scheduledElection != null && !scheduledElection.isDone()) {
				scheduledElection.cancel(true);
			}
			this.state = NodeState.LEADER;
			this.leaderId = serverId;

			restartHeartbeatTimer();
			appendEntries();
		}
	}

	private void restartHeartbeatTimer() {
		if (scheduledHeartbeat != null && !scheduledHeartbeat.isDone()) {
			scheduledHeartbeat.cancel(true);
		}
		logger.log(Level.FINE, "new heartbeat time is after {0} ms", heartbeatIntervalMilliseconds);
		scheduledHeartbeat = scheduledExecutorService.schedule(new Runnable() {
			@Override
			public void run() {
				appendEntries();
				restartHeartbeatTimer();
				//fix brain split issue, step down leader when no majority response back
			}
		}, heartbeatIntervalMilliseconds, TimeUnit.MILLISECONDS);
	}

	private void appendEntries() {
		for (final PeerServer peer : peerServers) {
			executorService.submit(new Runnable() {
				@Override
				public void run() {
					try {
						synchronized(peer) {
							appendEntries(peer);							
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
		}
	}

	private void appendEntries(PeerServer peer) {
		long firstLogIndex = raftLog.getFirstLogIndex();
		
		if (peer.getNextIndex() < firstLogIndex) {
			installSnapshot(peer);
		}
		long prevLogIndex = peer.getNextIndex() - 1;
		long prevLogTerm = 0;		
		if(prevLogIndex >  0) {
			prevLogTerm =  raftLog.getLogEntry(prevLogIndex).getTerm();
		}
		
		List<LogEntry> entries = new ArrayList<LogEntry>();
		long lastIndex = Math.min(raftLog.getLastLogIndex(), peer.getNextIndex() + maxLogEntriesPerRequest - 1);
		for (long index = peer.getNextIndex(); index <= lastIndex; index++) {
            LogEntry entry = raftLog.getLogEntry(index);
            entries.add(entry);
        }

		long leaderCommit = Math.min(commitIndex, prevLogIndex + entries.size());
		AppendEntriesRequest req = new AppendEntriesRequest(currentTerm, serverId, prevLogIndex, prevLogTerm, entries,
				leaderCommit);
		
		logger.log(Level.FINE, "appendEntries to {0} with {1}", new Object[] {peer.getEndPoint(), req.toJsonText()});
		
		String o = postRequest(peer, req);

		AppendEntriesResponse rsp = null;
		try {
			rsp = new AppendEntriesResponse(o);
		} catch (Exception e) {
			logger.log(Level.WARNING, "appendEntries response error from {0},json=''{1}''", new Object[]{peer.getEndPoint(), o});
			return;
		}
		
		logger.log(Level.FINE, "appendEntries resp from {0} term {1}, this term {2}", new Object[]{peer.getEndPoint(), rsp.getTerm(), currentTerm});

		if (rsp.getTerm() > currentTerm) {
			becomeFollower(rsp.getTerm());
		}else {
			if(rsp.isSuccess()) {
				//If successful: update nextIndex and matchIndex for follower
				peer.setMatchIndex(prevLogIndex + entries.size());
                peer.setNextIndex(peer.getMatchIndex() + 1);
                advanceCommitIndex();
			}else {
				//If AppendEntries fails because of log inconsistency: decrement nextIndex
				long i = peer.getNextIndex();
				peer.setNextIndex(i-1);
			}		
		}
	}

	private synchronized void becomeFollower(long term) {		
		this.currentTerm = term;
		votedFor = null;
		state = NodeState.FOLLOWER;
		if (scheduledHeartbeat != null && !scheduledHeartbeat.isDone()) {
			scheduledHeartbeat.cancel(true);
		}
		restartElectionTimer();
	}

	private  void advanceCommitIndex() {
		synchronized(commitMutex) {
			int peerSize = peerServers.size() + 1;
			long[] matchIndexes = new long[peerSize];
			int i = 0;
			for (PeerServer s : peerServers) {
				matchIndexes[i] = s.getMatchIndex();
				i = i + 1;
			}
			matchIndexes[i] = raftLog.getLastLogIndex();
			Arrays.sort(matchIndexes);
			long n = matchIndexes[peerSize / 2];

			if (raftLog.getLogEntry(n)!=null && raftLog.getLogEntry(n).getTerm() == currentTerm && n >  commitIndex) {
				long startIndex = commitIndex;
				commitIndex = n;
				for (long index = startIndex + 1; index <= n; index++) {
					LogEntry entry = raftLog.getLogEntry(index);
					stateMachine.apply(entry);
				}
				lastAppliedIndex = commitIndex;
				commitMutex.notifyAll();
			}			
		}
	}

	private void takeSnapshot() {
		logger.log(Level.INFO, "node is taking snapshot");
		stateMachine.writeSnapshot(); // in sync
		//discard the entire log up to that point
	}

	private void installSnapshot(PeerServer peer) {
		logger.log(Level.INFO, "install snapshot to peer server {0}", peer.getEndPoint());		
		Snapshot snapshot = stateMachine.readSnapshot();
		
		InstallSnapshotRequest request = new InstallSnapshotRequest(currentTerm, leaderId, snapshot.getLastIndex(),
				snapshot.getLastTerm(), 0, snapshot.getContents());
		
		//post to peer with http, content-type = text/json
		String s = postRequest(peer, request);
		InstallSnapshotResponse resp = new InstallSnapshotResponse(s);
		if(resp.getTerm()  > currentTerm) {
			becomeFollower(resp.getTerm());
		}
	}

	private String postRequest(PeerServer server, Message msg) {
		EndPoint endpoint = server.getEndPoint();
		String url = "http://" + endpoint.getHostname() + ":" + endpoint.getPort() + msg.getPath();
		HttpClient client = new HttpClient(url).setTimeOut(50);		
		String rsp = client.post(msg.toJsonText());
		return rsp;
	}

	/**
	 * Invoked by candidates to gather votes
	 */
	public synchronized VoteResponse voteService(VoteRequest request) {	
		boolean voteGranted = false;		
		
		boolean logIsOk = request.getLastLogTerm() > raftLog.getLastLogTerm()
                || (request.getLastLogTerm() == raftLog.getLastLogTerm()
                && request.getLastLogIndex() >= raftLog.getLastLogIndex());
		
		if (request.getTerm() >= currentTerm
				&& (votedFor == null || request.getCandidateId().equals(votedFor))
				&& logIsOk) {
			voteGranted = true;
			votedFor =  request.getCandidateId();
		}
		
		logger.log(Level.INFO, "vote granted={0}.request=''{1}'',and this lastTerm={2},lastIndex={3}",
				new Object[] {voteGranted, request.toJsonText(), raftLog.getLastLogTerm(), raftLog.getLastLogIndex() });
		
		VoteResponse resp = new VoteResponse(currentTerm, voteGranted);		
		return resp;
	}

	/**
	 * Follower Service: Invoked by leader to replicate log entries; also used as heartbeat
	 */
	public AppendEntriesResponse appendEntriesService(AppendEntriesRequest request) {
		synchronized(this) {
			AppendEntriesResponse resp = new AppendEntriesResponse(currentTerm, false);
			
			logger.log(Level.FINE, "appendEntriesService request=''{0}'',and this lastLogIndex={1},term={2}",
					new Object[] { request.toJsonText(), raftLog.getLastLogIndex(), currentTerm });
						
			if(request.getTerm() < currentTerm) {
				return resp;
			}
			becomeFollower(request.getTerm());
							
			if (request.getPrevLogIndex() > raftLog.getLastLogIndex()) {
				return resp;
			}
			
			//If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
			List<LogEntry> entries = new ArrayList<LogEntry>();
			for(LogEntry e: request.getLogEntries()) {
				if(e.getIndex() < raftLog.getFirstLogIndex()) {
					continue;
				}
				
				if(e.getIndex() <= raftLog.getLastLogIndex()) {
					LogEntry l =  raftLog.getLogEntry(e.getIndex());
					if(l !=null && l.getTerm() == e.getTerm()) {
						continue;
					}
					logger.log(Level.FINE, "appendEntriesService delete raftlog after index {0}", e.getIndex());
					raftLog.deleteAfter(e.getIndex());
				}
				entries.add(e);
			}
			
			//Append any new entries not already in the log
			raftLog.appendLogEntry(entries);
			
			//If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
			if(request.getLeaderCommit()  > commitIndex) {
				commitIndex = Math.min(request.getLeaderCommit(), raftLog.getLastLogIndex());
			}
			
			if (lastAppliedIndex < commitIndex) {
				for (long index = lastAppliedIndex + 1; index <= commitIndex; index++) {
					LogEntry entry = raftLog.getLogEntry(index);
					if (entry != null) {
						stateMachine.apply(entry);
					}
					lastAppliedIndex = index;
				}
			}
			
			leaderId = request.getServerId();
			resp.setSuccess(true);	
			return resp;			
		}
	}

	/**
	 * Follower Service: Invoked by leader to send a snapshot to a follower.
	 */
	public InstallSnapshotResponse installSnapshotService(InstallSnapshotRequest request) {
		synchronized (this) {
			InstallSnapshotResponse resp = new InstallSnapshotResponse(currentTerm);
			logger.log(Level.INFO, "installShanpshot request from {0}", request.getServerId());

			// Reply immediately if term < currentTerm
			if (request.getTerm() < currentTerm) {
				return resp;
			}

			becomeFollower(request.getTerm());
			// Save snapshot file
			Snapshot snapshot = new Snapshot(request.getData());
			// If existing log entry has same index and term as snapshot’s last included
			// entry, retain log entries following it and reply
			LogEntry e = raftLog.getLogEntry(request.getLastIncludedIndex());
			if (e != null && e.getTerm() == request.getLastIncludedTerm()) {
				raftLog.deleteBefore(request.getLastIncludedIndex());
			} else {
				// Discard the entire log
				raftLog.reset(request.getLastIncludedIndex());
			}

			// Reset state machine using snapshot contents
			stateMachine.loadSnapshot(snapshot);
			return resp;
		}
	}

	/**
	 * Leader service: Invoked by client to send data to leader.
	 */
	public String writeService(String record) {
		JSONObject obj = new JSONObject();
		boolean result = false;
		
		if(state != NodeState.LEADER) {
			if(leaderId == null) {
				obj.put("code", 500);
				obj.put("message", "no leader found in this cluster");
				return obj.toString(2);
			}
			
			logger.log(Level.INFO, "redirect to leader {0},and only leader can accept data writing", leaderId);
			String url = "http://" + leaderId.getHostname() + ":" + leaderId.getPort() + "/raft/writerequest";
			HttpClient client = new HttpClient(url);
			String resp = client.post(record);
			return resp;
		}
		
		synchronized(commitMutex) {
			try {
				//add residue to logentry, and applied to leader.
				LogEntry entry = new LogEntry(currentTerm, new JSONObject(record));
				long entryIndex = raftLog.appendLogEntry(entry);
				
				appendEntries();
				result = true;
				
				if (!asyncWrite) { // sync wait commitIndex >= newLastLogIndex
				    long startTime = System.currentTimeMillis();
				    while (lastAppliedIndex < entryIndex) {
				        if (System.currentTimeMillis() - startTime >= 200) {
				            break;
				        }
						// Give up the lock and go to sleep until some other thread enters the same
						// monitor and calls notify().
				        commitMutex.wait(200); 
				    }
				    
			        if (lastAppliedIndex < entryIndex) {
			        	result = false;
			        }
				}
			} catch (Exception e) {
				logger.log(Level.SEVERE, "write Service error happened!!!", e);
				result = false;
				obj.put("message", e.getMessage());
			}			
		}

		obj.put("success", result);
		return obj.toString(2);
	}
	
	public StateMachine getStateMachine() {
		return stateMachine;
	}


}
