package com.dataleading.jraft;

/**
 *        
 * @author shshen
 *
 */
public interface StateMachine {
	

	/**
	 * Write the entire current system state to to a snapshot on stable storage, 
	 * then the entire log up to that point is discarded.
	 */
	public void writeSnapshot();
	
	public Snapshot readSnapshot();

	/**
	 * Reset state machine using snapshot contents
	 */
	public void loadSnapshot(Snapshot snapshot);

	public void apply(LogEntry logEntry);
	
	public long getLastIndex();
	
	public long getLastTerm();
	
}
