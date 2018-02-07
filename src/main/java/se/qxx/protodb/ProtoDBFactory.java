package se.qxx.protodb;

import org.apache.commons.lang3.StringUtils;

import se.qxx.protodb.backend.Drivers;
import se.qxx.protodb.backend.MysqlBackend;
import se.qxx.protodb.backend.SqliteBackend;
import se.qxx.protodb.exceptions.DatabaseNotSupportedException;

public class ProtoDBFactory {

	public static ProtoDB getMysqlInstance(String host, String user, String password, String database) {
		return getMysqlInstance(host, user, password, database, "");
			
	}
	
	public static ProtoDB getMysqlInstance(String host, String user, String password, String database, String logFilename) {
		return new ProtoDB(
				new MysqlBackend(host, user, password, database),
				logFilename);
			
	}
	
	public static ProtoDB getSqliteInstance(String databaseFilename) {
		return new ProtoDB(
				new SqliteBackend(databaseFilename),
			"");
	}
	
	public static ProtoDB getInstance(String driver, String connectionString) throws DatabaseNotSupportedException {
		String overriddenConnectionString = getOverriddenConnectionString(driver, connectionString);
		if (isMySql(driver))
			return new ProtoDB(
					new MysqlBackend(connectionString), "");
		
		if (isSqlite(driver))
			return new ProtoDB(
				new SqliteBackend(connectionString), "");
			

		throw new DatabaseNotSupportedException();

	}
	
	private static String getOverriddenConnectionString(String driver, String connectionString) {
		String property = "connectionString";
		String result = connectionString;
		if (isMySql(driver))
			property = "mysqlConnectionString";
		
		if (isSqlite(driver))
			property = "sqliteConnectionString";
		
		String propValue = System.getProperty(property);
		if (!StringUtils.isEmpty(propValue))
			return propValue;
		
		return result;
	}
	
	public static boolean isMySql(String driver) {
		return StringUtils.equalsIgnoreCase(driver, Drivers.MYSQL);
	}
	
	public static boolean isSqlite(String driver) {
		return StringUtils.equalsIgnoreCase(driver, Drivers.SQLITE);
	}
}