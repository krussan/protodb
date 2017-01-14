package se.qxx.protodb.test;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import se.qxx.protodb.ProtoDB;
import se.qxx.protodb.exceptions.SearchFieldNotFoundException;

public class TestExcludingObjects {

	ProtoDB db = null;

	private final String DATABASE_FILE = "protodb_select_test.db";
	
	@Before
	public void Setup() {		
	    db = new ProtoDB(DATABASE_FILE);
	}
	
	@Test
	public void TestExcluding() {	
		try {
			List<String> excludedObjects = new ArrayList<String>();
			excludedObjects.add("testOne");
			
			List<TestDomain.ObjectOne> result =
				db.find(
					TestDomain.ObjectOne.getDefaultInstance(), 
					"testOne.bb", 
					true, 
					false,
					excludedObjects);
			
			// we should get one single result..
			assertEquals(1, result.size());
		
			TestDomain.ObjectOne b = result.get(0);
			assertEquals(b.getOois(), 986);
			
			TestDomain.SimpleTest o1 = b.getTestOne();
			assertNull(o1);
			
		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
