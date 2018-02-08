package se.qxx.protodb.backend;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class MysqlBackend extends DatabaseBackend {
	
	public MysqlBackend(String connectionString) {
		this.addTypeMap(JDBCType.FLOAT, "REAL");
		
		this.setDriver(Drivers.MYSQL);
		this.setConnectionString(connectionString);
	}
	
	public MysqlBackend(String host, String user, String password, String database) {
		this.setDriver(Drivers.MYSQL);
		this.setConnectionString(
				String.format("jdbc:mysql://%s/%s?user=%s&password=%s",
						host,
						user,
						password,
						database));
	}
	
	@Override
	public String getStartBracket() {
		return "`";
	}
	
	@Override
	public String getEndBracket() {
		return "`";
	}

	@Override
	public String getIdentityDefinition() {
		return "ID INT NOT NULL AUTO_INCREMENT PRIMARY KEY";
	}

	@Override
	public int getIdentityValue(Connection conn) throws SQLException {
		PreparedStatement prep = conn.prepareStatement("SELECT LAST_INSERT_ID()");
		ResultSet rs = prep.executeQuery();
		
		if (rs.next())
			return rs.getInt(1);
		else
			return -1;
	}
}
