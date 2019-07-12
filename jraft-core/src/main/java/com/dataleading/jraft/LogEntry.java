package com.dataleading.jraft;

import org.json.JSONObject;

public class LogEntry {

	private long index;
	private long term;
	private JSONObject data;

	public LogEntry(long term, JSONObject data) {
		this.term =  term;
		this.data = data;
	}

	public long getTerm() {
		return term;
	}
	
	public void setIndex(long index) {
		this.index = index;
	}
	public long getIndex() {
		return index;
	}
	public JSONObject getData() {
		return data;
	}

	public JSONObject toJson() {
		JSONObject json = new JSONObject();
		json.put("term", term);
		json.put("index", index);
		json.put("data", data);
		return json;
	}
	
}
