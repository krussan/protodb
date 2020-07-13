package se.qxx.protodb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;

import java.sql.DatabaseMetaData;

import se.qxx.protodb.backend.Drivers;
import se.qxx.protodb.backend.MysqlBackend;
import se.qxx.protodb.backend.SqliteBackend;
import se.qxx.protodb.exceptions.DatabaseNotSupportedException;

public class ProtoDBFactory {
	
	public static ProtoDB getInstance(String driver, String connectionString) throws DatabaseNotSupportedException {
		return getInstance(driver, connectionString, "");
	}

	public static ProtoDB getInstance(String driver, String connectionString, String logFilename) throws DatabaseNotSupportedException {
		DBType type = getDatabaseType(driver, connectionString);

		if (type == DBType.Mysql)
			return new ProtoDB(
					new MysqlBackend(driver, connectionString), logFilename);
		
		if (type == DBType.Sqlite)
			return new ProtoDB(
				new SqliteBackend(driver, connectionString), logFilename);

		throw new DatabaseNotSupportedException();

	}
	
	public static DBType getDatabaseType(String driver, String connectionString) {
		try {
			String productName = getDatabaseProductName(driver, connectionString);

			System.out.println(productName);
			if (StringUtils.containsIgnoreCase(productName, "mysql"))
				return DBType.Mysql;
			else if (StringUtils.containsIgnoreCase(productName, "sqlite"))
				return DBType.Sqlite;
			else
				return DBType.Unsupported;
			
		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			System.out.println(e.toString());
			return DBType.Unsupported;
		}
	}
	
	public static boolean isSqlite(String driver) {
		return StringUtils.equalsIgnoreCase(driver, Drivers.SQLITE);
	}
	
	public static String getDatabaseProductName(String driver, String connectionString) throws ClassNotFoundException, SQLException {
		Connection conn = null;

		try {
			Class.forName(driver);
		    conn = DriverManager.getConnection(connectionString);

		    DatabaseMetaData metaData = conn.getMetaData();
		    return metaData.getDatabaseProductName();
		    
		}
		finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
			}
		}
	}
}