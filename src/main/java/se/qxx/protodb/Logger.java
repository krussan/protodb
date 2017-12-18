package se.qxx.protodb;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;

public class Logger {
	private static String logfile = "";
	
	//---------------------------------------------------------------------------------
	//----------------------------------------------------------------------  LOG
	//---------------------------------------------------------------------------------

	public static void log(String message) {
		log(message, null);
	}
	
	public static void log(String message, Exception e) {
		logMessage(getLogString(message));
	}
	

	public static void printStackTrace(Exception e) {
		for (StackTraceElement ste : e.getStackTrace()) {
			logMessage(String.format("%s\n", ste));
		}
	}

	private static void logMessage(String logMessage) {
		if (!StringUtils.isEmpty(getLogfile())) {
			try {
				java.io.FileWriter fs = new java.io.FileWriter(getLogfile(), true);
				fs.write(String.format("%s\n", logMessage));
				fs.close();
			}
			catch (Exception ex) {
				System.out.println("---Exception occured in logging class---");
				ex.printStackTrace();
			}
		}
	}

	public static String getLogfile() {
		return logfile;
	}

	public static void setLogfile(String logfile) {
		logfile = logfile;
	}

	private static String getLogString(String msg) {
		return String.format("%s - [%s] - %s", getDateString(), Thread.currentThread().getId(), msg);
	}

	public static String getDateString() {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	    Date date = new Date();
	    return dateFormat.format(date);
	}

}
