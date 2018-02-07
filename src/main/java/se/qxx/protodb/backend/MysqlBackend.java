package se.qxx.protodb.backend;

import java.util.List;

public class MysqlBackend extends DatabaseBackend {
	
	public MysqlBackend(String connectionString) {
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
	public String getIdentityDefinition() {
		return "ID INT NOT NULL AUTO_INCREMENT";
	}
}
