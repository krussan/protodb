package se.qxx.protodb.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import se.qxx.protodb.DBType;
import se.qxx.protodb.ProtoDB;
import se.qxx.protodb.ProtoDBFactory;
import se.qxx.protodb.exceptions.DatabaseNotSupportedException;

public class TestBase {
		
	public static void clearDatabase(ProtoDB db, String connectionString) throws ClassNotFoundException, SQLException {
    	if (db.getDBType() == DBType.Sqlite) {
    		String[] splitted = StringUtils.split(connectionString, ":");
    		ArrayUtils.reverse(splitted);
    		
    		if (splitted.length > 0) {
	    		File f = new File(splitted[0]);
	    		if (f.exists())
	    			f.delete();
    		}
    	}
    	
    	if (db.getDBType() == DBType.Mysql) {
    		db.dropAllTables();
    	}

	}
	
	protected static Collection<Object[]> getParams() {
		return Arrays.asList(TestConstants.TEST_PARAMS);
	}
	
	
	protected static Collection<Object[]> getParams(String paramsProperty) {
		if (System.getProperties().containsKey(paramsProperty)) {
			return readParamsFile(System.getProperty(paramsProperty));
		}
		else {
			return Arrays.asList(TestConstants.TEST_PARAMS);
		}
	}

	/***
	 * Reads test params from file.
	 * Each row defines arguments for a parameterized class
	 * Each object should be separated by semi-colon;
	 * @param filename
	 * @return
	 */
	protected static Collection<Object[]> readParamsFile(String filename) {
		File file = new File(filename);
		Collection<Object[]> result = new ArrayList<Object[]>();
		FileReader fr = null;

		try {
			if (file.exists()) {
				fr = new FileReader(file);
				BufferedReader br = new BufferedReader(fr);
				StringBuilder sb = new StringBuilder();
				String line;
				
				while ((line = br.readLine()) != null) {
					result.add(
						StringUtils.split(line, ";"));
				}
				
				return result;
			}
		
		}
		catch (IOException e) {
		}
		finally {
			try {
				if (fr != null)
					fr.close();
			} catch (IOException e) {}
		}
		
		return Arrays.asList(TestConstants.TEST_PARAMS);
	}
}
