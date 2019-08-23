package com.dataleading.raft;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import com.dataleading.raft.OptionGroup.MultiKeyMap;
import com.dataleading.raft.OptionGroup.Option;


public class CommandParser {

	private OptionGroup options;
	private String command;
	private List<Option> optionList;

	public CommandParser(OptionGroup options, String[] args){
		this.options = options;
		this.optionList = new ArrayList<Option>();
		parse(args);
		validate();
	}

	// short options (-S, -SV, -S V, -S=V, -SV1=V2, -S1S2)
	// long or partial long options (--L, -L, --L=V, -L V, -L=V, --l, --l=V)
	// we throw RuntimeException here.
	private void parse(String[] args) throws IllegalArgumentException{
		if(args.length<1 || options.getByCommand(args[0])==null){
			throw new IllegalArgumentException("Illegal Arguments: incorrect command");
		}
		command = args[0];
		
		MultiKeyMap optMap = options.getByCommand(command);
		Option currentOpt = null;

		for (int i = 1; i < args.length; i++) {
			String optStr = args[i];
			int eqIdx = optStr.indexOf("=");
			if (optStr.startsWith("--")) {
				if (eqIdx > 0) {
					String longName = optStr.substring(2, eqIdx);
					String value = optStr.substring(eqIdx+1);
					currentOpt = optMap.get(longName);
					if(currentOpt!=null){
						currentOpt.setValue(value);
						optionList.add(currentOpt);	
						if(!value.isEmpty()) {
							currentOpt = null;
						}
					}
				}else{
					String longName = optStr.substring(2);
					currentOpt = optMap.get(longName);
					if(currentOpt!=null){
						optionList.add(currentOpt);						
					}					
				}
			} else if (optStr.startsWith("-")) {
				if (eqIdx > 0) {
					String shortName = optStr.substring(1, eqIdx);
					String value = optStr.substring(eqIdx+1);
					currentOpt = optMap.get(shortName);
					if(currentOpt!=null){
						currentOpt.setValue(value);
						optionList.add(currentOpt);		
						if(!value.isEmpty()) {
							currentOpt = null;
						}
					}
				}else{
					String shortName = optStr.substring(1);
					currentOpt = optMap.get(shortName);
					if(currentOpt!=null){
						optionList.add(currentOpt);						
					}					
				}
			} else {
				//process non-option argument
				if (currentOpt != null) {
					if(currentOpt.hasOptionalArg()) {						
						if(eqIdx == 0) {
							optStr = optStr.substring(1);
						}
						
						if(!optStr.isEmpty()) {
							currentOpt.setValue(optStr);
							currentOpt = null;
						}
					}else {
						currentOpt = null;
					}					
				} else {
					String defaultOption = optMap.getDefaultOption();
					if(defaultOption != null && !"".equals(defaultOption)) {
						currentOpt = optMap.get(defaultOption);
						currentOpt.setValue(optStr);
						optionList.add(currentOpt);	
						currentOpt = null;
					}
				}
			}
		}
		
	}
	
	//1. validate the required options
	//2. validate the option has argument
	private void validate(){
		MultiKeyMap optMap = options.getByCommand(command);
		Collection<Option> values = optMap.values();
		for(Option o: values){
			if(o.isRequired()){
				if(!optionList.contains(o)){
					throw new IllegalArgumentException("Illegal Arguments: " + o.getLongOpt() + " is required!");
				}
			}
		}
		for(Option o: optionList){
			if(o.hasOptionalArg()){
				String value = o.getValue();
				if(value==null || "".equals(value)){
					throw new IllegalArgumentException("Illegal Arguments: " + o.getLongOpt() + " value is required!");
				}
			}
		}
	}

	public boolean hasOption(String optName){
		boolean has = false;
		MultiKeyMap optMap = options.getByCommand(command);
		Option opt = optMap.get(optName);
		if(opt !=null ){
			has = optionList.contains(opt);
		}
		return has;
	}
	
	public String getOptionValue(String optName){
		MultiKeyMap optMap = options.getByCommand(command);
		Option opt = optMap.get(optName);

		return opt.getValue();
	}
	
	public String getCommand() {
		return command;
	}

	public void printHelp(){
		options.printHelp();
	}

}
