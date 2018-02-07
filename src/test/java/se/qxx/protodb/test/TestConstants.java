package se.qxx.protodb.test;

import se.qxx.protodb.backend.Drivers;

public class TestConstants {

	public static final Object[][] TEST_PARAMS = {     
        { Drivers.SQLITE, "protodb_test.db" }, { Drivers.MYSQL, "jdbc:mysql://192.168.1.120/protodb?user=protodb&password=protodb" }  
	};
	
	public static final Object[][] TEST_PARAMS_MYSQL = {     
		{ Drivers.MYSQL, "jdbc:mysql://192.168.1.120/protodb?user=protodb&password=protodb" }  
	};
}
