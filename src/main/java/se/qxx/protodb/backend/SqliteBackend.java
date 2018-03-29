package se.qxx.protodb.backend;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import se.qxx.protodb.DBType;

public class SqliteBackend extends DatabaseBackend {


	public SqliteBackend(String driver, String connectionString) {
		super(driver, connectionString);
		
		this.addTypeMap(JDBCType.DOUBLE, "FLOAT");
		this.addTypeMap(JDBCType.BIGINT, "INTEGER");
		this.addTypeMap(JDBCType.BIT, "INTEGER");
		this.addTypeMap(JDBCType.LONGVARCHAR, "VARCHAR");
		this.addTypeMap(JDBCType.LONGVARBINARY, "VARCHAR");
		this.addTypeMap(JDBCType.BLOB, "BLOB");
	}

	@Override
	public String getIdentityDefinition() {
		return "ID INTEGER PRIMARY KEY AUTOINCREMENT";
	}

	@Override
	public int getIdentityValue(Connection conn) throws SQLException {
		PreparedStatement prep = conn.prepareStatement("SELECT last_insert_rowid()");
		ResultSet rs = prep.executeQuery();
		
		if (rs.next())
			return rs.getInt(1);
		else
			return -1;
	}
	
	@Override
	public DBType getDBType() {
		return DBType.Sqlite;
	}
}
