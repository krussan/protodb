package se.qxx.protodb.backend;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public abstract class DatabaseBackend {
	private String driver;
	private String connectionString;
	
	public String getDriver() {
		return driver;
	}
	public void setDriver(String driver) {
		this.driver = driver;
	}
	public String getConnectionString() {
		return connectionString;
	}
	public void setConnectionString(String connectionString) {
		this.connectionString = connectionString;
	}
	
	public List<ColumnDefinition> getColumns(Connection conn) throws SQLException {
		List<ColumnDefinition> list = new ArrayList<ColumnDefinition>();
		DatabaseMetaData metadata = conn.getMetaData();
	    ResultSet resultSet = metadata.getColumns(null, null, "users", null);
	    while (resultSet.next()) {
	    	String name = resultSet.getString("COLUMN_NAME");
    		String type = resultSet.getString("TYPE_NAME");
    		list.add(new ColumnDefinition(name, type));
//    		int size = resultSet.getInt("COLUMN_SIZE");
	    }
	    
	    return list;
	}

	public boolean tableExist(String tableName, Connection conn) throws SQLException {
		DatabaseMetaData meta = conn.getMetaData();
		ResultSet res = meta.getTables(null, null, tableName, new String[] {"TABLE"});
		if (res.getFetchSize() > 0)
			return true;
		else
			return false;
	}
	public abstract String getIdentityDefinition();
}