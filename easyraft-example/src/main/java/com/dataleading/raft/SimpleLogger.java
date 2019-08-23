package com.dataleading.raft;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;


public class SimpleLogger {
	
	private static SimpleLogger instance;

	public static SimpleLogger getStdInstance(){
		if(instance==null){
			instance = new SimpleLogger();
			instance.setup(Level.INFO);
		}
		return instance;
	}
	
	private SimpleLogger(){
		
	}
	
	public void setLevel(Level level) {
		Logger rootLogger = Logger.getLogger("com.dataleading");
		rootLogger.setLevel(level);
		
		Handler[] handlers = rootLogger.getHandlers();
		for(Handler h :handlers) {
			h.setLevel(level);
		}
	}

	private void setup(Level level){
		Logger rootLogger = Logger.getLogger("com.dataleading");
		rootLogger.setUseParentHandlers(false);

		StdConsoleHandler consoleHandler = new StdConsoleHandler();
		consoleHandler.setLevel(level);
		consoleHandler.setFormatter(new MyFormatter());
		
		rootLogger.setLevel(level);
		rootLogger.addHandler(consoleHandler);	
	}

	//refer to java.util.logging.SimpleFormatter;
	static class MyFormatter extends Formatter {
		private final DateFormat df = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
		
		public String format(LogRecord record) {
	        StringBuffer buffer = new StringBuffer();
	        String l = record.getLevel().getName();
	        buffer.append(df.format(new Date(record.getMillis()))).append(" - [");  
	        
	        buffer.append(fixedLength(l));
	        buffer.append("] ");
	        buffer.append(formatMessage(record));
	        String throwable = "";
	        if (record.getThrown() != null) {
	            StringWriter sw = new StringWriter();
	            PrintWriter pw = new PrintWriter(sw);
	            pw.println();
	            record.getThrown().printStackTrace(pw);
	            pw.close();
	            throwable = sw.toString();
	        }
	        buffer.append(throwable);
	        buffer.append("\n");
	        return buffer.toString();
		}
		
//	    public String getHead(Handler h) {
//	        return super.getHead(h);
//	    }
//	    public String getTail(Handler h) {
//	        return super.getTail(h);
//	    }
	}	
	
	static class StdConsoleHandler extends ConsoleHandler {
		protected void setOutputStream(OutputStream out) {
			super.setOutputStream(out); // kitten killed here :-(
		}
	}
	
	public static String fixedLength(String string) {
	    return String.format("%1$-7s", string);
	}

}