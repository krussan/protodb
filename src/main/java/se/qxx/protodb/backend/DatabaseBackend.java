package se.qxx.protodb.backend;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import se.qxx.protodb.DBType;

public abstract class DatabaseBackend {
	private String driver;
	private String connectionString;
	private Map<JDBCType, String> typeMap = new HashMap<>();
	
	public DatabaseBackend(String driver, String connectionString) {
		this.setDriver(driver);
		this.setConnectionString(connectionString);
	}
	
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

	public String getStartBracket() {
		return "[";
	}
	
	public String getEndBracket() {
		return "]";
	}
	
	public List<ColumnDefinition> getColumns(String tableName, Connection conn) throws SQLException {
		List<ColumnDefinition> list = new ArrayList<ColumnDefinition>();
		DatabaseMetaData metadata = conn.getMetaData();
	    ResultSet resultSet = metadata.getColumns(null, null, tableName, null);
	    while (resultSet.next()) {
	    	String name = resultSet.getString("COLUMN_NAME");
	    	int jType = resultSet.getInt("DATA_TYPE");
    		String type = JDBCType.valueOf(jType).getName(); 
    				//resultSet.getString("DATA_TYPE");
    		list.add(new ColumnDefinition(name, type));
//    		int size = resultSet.getInt("COLUMN_SIZE");
	    }
	    
	    return list;
	}

	public boolean tableExist(String tableName, Connection conn) throws SQLException {
		DatabaseMetaData meta = conn.getMetaData();
		ResultSet res = meta.getTables(null, null, tableName, null);
		
		if (res.next())
			return true;
		else
			return false;
	}
	
	public List<String> getAllTables(Connection conn) throws SQLException {
		DatabaseMetaData meta = conn.getMetaData();
		ResultSet rs = meta.getTables(null, null, "%", null);
		List<String> result = new ArrayList<String>();
		
		while (rs.next()) {
			String schema = rs.getString("TABLE_SCHEMA");
			String table = rs.getString("TABLE_NAME");
			if (!StringUtils.equalsIgnoreCase(schema, "INFORMATION_SCHEMA"))
				result.add(table);
		}
		
		return result;
		
	}

	public abstract String getIdentityDefinition();

	public abstract int getIdentityValue(Connection conn) throws SQLException;
	
	public abstract DBType getDBType();
	
	public abstract String getMD5Function();
	
	public String getTypeMap(JDBCType type) {
		if (typeMap.containsKey(type))
			return typeMap.get(type);
		else
			return type.getName();
	}
	
	public void addTypeMap(JDBCType type, String typeName) {
		typeMap.put(type, typeName);
	}
	
	public String getEscapeString() {
		return "ESCAPE '\\'";
	}
	
	//getTypeMap
	//DOUBLE -> FLOAT
	//FLOAT -> REAL
	//INTEGER -> INTEGER (not in map)
}
