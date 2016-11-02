package com.angejia.dw.sqoop;

import java.io.ByteArrayOutputStream;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;

import com.angejia.dw.DWLogger;

public class SqoopRunner {
	private DWLogger logger;
	
	public DWLogger getLogger() {
		return logger;
	}

	public void setLogger(DWLogger logger) {
		this.logger = logger;
	}
	
	public long begin(String themeName, String moduleName) {
		long time = System.currentTimeMillis();
		logger.log(time, themeName, moduleName, "", "Begin", "", "");
        return time;
	}
	
	public long end(String themeName,  String moduleName){
        long time = System.currentTimeMillis();
        logger.log(time, themeName, moduleName, "", "End", "", "");
        return time;
    }
	
	public void runSqoopCommandsFromCMD(String sqoopCommands, String fileName,
			String themeName, String moduleName ) {
		long startTime = System.currentTimeMillis();
		String action = "SQOOP";
		
		DefaultExecutor executor = new DefaultExecutor();
		
		executor.setExitValues(null);  
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();  
        ByteArrayOutputStream errorStream = new ByteArrayOutputStream(); 
        
        PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream,errorStream);  
        executor.setStreamHandler(streamHandler);
        
        try {
			executor.execute(CommandLine.parse(sqoopCommands));
		} catch (Exception e) {
			e.printStackTrace();
			logger.log(startTime, themeName, moduleName, action, "Exception", fileName, e.getMessage());
		}
		
		String stdErr = errorStream.toString();
		if(stdErr.contains("ERROR")) {
			logger.log(startTime, themeName, moduleName, action, "Exception", fileName, stdErr.substring(stdErr.indexOf("ERROR")));
		} else {
			logger.log(startTime, themeName, moduleName, action, "RunStop", fileName, "OK");
		}
	}
	
}
