package se.qxx.protodb.backend;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class SqliteBackend extends DatabaseBackend {

	public static final String DRIVER = "org.sqlite.JDBC";
	
	public SqliteBackend(String databaseFilename) {
		this.setDriver(DRIVER);
		this.setConnectionString(
				String.format("jdbc:sqlite:%s", databaseFilename));
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
}
