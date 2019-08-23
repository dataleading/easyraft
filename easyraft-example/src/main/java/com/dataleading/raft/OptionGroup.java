package com.dataleading.raft;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class OptionGroup {

	private Map<String, MultiKeyMap> cmdMap;
	private Map<String, String> descMap;

	public OptionGroup() {
		cmdMap = new HashMap<String, MultiKeyMap>();
		descMap = new HashMap<String, String>();
	}

	public OptionGroup enableNameServer() {
		MultiKeyMap dataOpts = new MultiKeyMap();

		Option peers = new Option("s", "peers", "Raft consensus peer servers").setRequired(true).setOptionalArg(true);
		Option port = new Option("p", "port", "Master listening port number").setRequired(true).setOptionalArg(true);
		Option level = new Option("l", "level", "Set log level {FINE,INFO,WARN,ERROR}");

		dataOpts.put(peers);
		dataOpts.put(port);
		dataOpts.put(level);

		cmdMap.put("start", dataOpts);
		descMap.put("start", "[start] masterserver for manageing the cluster");
		return this;
	}

	public MultiKeyMap getByCommand(String c) {
		return cmdMap.get(c);
	}

	public class MultiKeyMap {

		private String defaultOption;
		private Map<String, Option> options;

		public MultiKeyMap() {
			options = new HashMap<String, Option>();
		}

		public MultiKeyMap put(Option value) {
			options.put(value.getShortOpt(), value); // short option
			options.put(value.getLongOpt(), value); // long option
			return this;
		}

		public Option get(String key) {
			return options.get(key);
		}

		public Set<Option> values() {
			Set<Option> optSet = new HashSet<Option>();
			Collection<Option> optCol = options.values();
			for (Option o : optCol) {
				if (!optSet.contains(o)) {
					optSet.add(o);
				}
			}
			return optSet;
		}

		public String getDefaultOption() {
			return defaultOption;
		}

		public void setDefaultOption(Option defaultOption) {
			this.defaultOption = defaultOption.getLongOpt();
		}

	}

	public void printHelp() {
		System.out.println("Usage: [command] [options]");
		Set<String> keySet = cmdMap.keySet();
		for (String k : keySet) {
			String desc = descMap.getOrDefault(k, "");
			System.out.println(desc);
			MultiKeyMap optMap = getByCommand(k);
			Collection<Option> optList = optMap.values();
			for (Option o : optList) {
				String s = String.format("  -%-1s,--%-12s%s", o.getShortOpt(), o.getLongOpt(), o.getDescription());
				if (o.hasOptionalArg()) {
					s = String.format("  -%-1s,--%-12s%s", o.getShortOpt(), o.getLongOpt() + "=", o.getDescription());
				}
				System.out.println(s);
			}
		}
	}

	public static class Option {
		private String shortOpt;
		private String longOpt;
		private boolean optionalArg; // specifies whether the argument value of this Option is optional
		private boolean required;
		private String description;
		private String value; // support multiple args
		// private List<String> values = new ArrayList<String>();
		// private int numberOfArgs = -1;

		public Option(String shortOpt, String longOpt, String description) {
			this.shortOpt = shortOpt;
			this.longOpt = longOpt;
			this.description = description;
		}

		public String getShortOpt() {
			return shortOpt;
		}

		public void setShortOpt(String shortOpt) {
			this.shortOpt = shortOpt;
		}

		public String getLongOpt() {
			return longOpt;
		}

		public void setLongOpt(String longOpt) {
			this.longOpt = longOpt;
		}

		public boolean hasOptionalArg() {
			return optionalArg;
		}

		public Option setOptionalArg(boolean hasArg) {
			this.optionalArg = hasArg;
			return this;
		}

		public boolean isRequired() {
			return required;
		}

		public Option setRequired(boolean required) {
			this.required = required;
			return this;
		}

		public String getDescription() {
			return description;
		}

		public void setDescription(String description) {
			this.description = description;
		}

		public void setValue(String v) {
			value = v;
		}

		public String getValue() {
			return value;
		}

		@Override
		public String toString() {
			return "Option [shortOpt=" + shortOpt + ", longOpt=" + longOpt + ", value=" + value + "]";
		}

	}

}
