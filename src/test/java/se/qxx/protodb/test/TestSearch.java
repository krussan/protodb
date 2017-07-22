package se.qxx.protodb.test;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import se.qxx.protodb.ProtoDB;
import se.qxx.protodb.exceptions.SearchFieldNotFoundException;

public class TestSearch {
	ProtoDB db = null;
	
	private final String DATABASE_FILE = "protodb_select_test.db";
	
	@Before
	public void Setup() {		
	    db = new ProtoDB(DATABASE_FILE);
	}
	
	@Test
	public void TestSearchExact() {	
		try {
			List<TestDomain.SimpleTwo> result =
				db.find(
					TestDomain.SimpleTwo.getDefaultInstance(), 
					"director", 
					"madeByAnotherDirector", 
					false);
			
			// we should get one single result..
			assertEquals(1, result.size());
		
			TestDomain.SimpleTwo b = result.get(0);

			assertEquals("thisIsAlsoATitle", b.getTitle());
			assertEquals("madeByAnotherDirector", b.getDirector());
			
//			PreparedStatement prep = "SELECT * FROM SimpleTest";
//			
//			testTableStructure(db, "SimpleTest", SIMPLE_FIELD_NAMES, SIMPLE_FIELD_TYPES);
		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException  e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
}
