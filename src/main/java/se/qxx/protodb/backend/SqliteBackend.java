package se.qxx.protodb.backend;

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
}
