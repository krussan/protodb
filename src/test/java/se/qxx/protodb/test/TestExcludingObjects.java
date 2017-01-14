package se.qxx.protodb.test;

import static org.junit.Assert.*;

import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

import se.qxx.protodb.ProtoDB;

public class TestExcludingObjects {

	ProtoDB db = null;

	private final String DATABASE_FILE = "protodb_select_test.db";
	
	@Before
	public void Setup() {		
	    db = new ProtoDB(DATABASE_FILE);
	}
	
	@Test
	public void TestSimple() {	
		try {
			TestDomain.RepObjectOne b = db.get(1, TestDomain.RepObjectOne.getDefaultInstance());

			// happyCamper should be 3
			assertEquals(3, b.getHappycamper());
			
			// we should have two repeated objects
			assertEquals(2, b.getListOfObjectsCount());
			
			TestDomain.SimpleTwo o1 = b.getListOfObjects(0);
			assertEquals("thisIsATitle", o1.getTitle());
			assertEquals("madeByThisDirector", o1.getDirector());
			
			TestDomain.SimpleTwo o2 = b.getListOfObjects(1);
			assertEquals("thisIsAlsoATitle", o2.getTitle());
			assertEquals("madeByAnotherDirector", o2.getDirector());
			
//			PreparedStatement prep = "SELECT * FROM SimpleTest";
//			
//			testTableStructure(db, "SimpleTest", SIMPLE_FIELD_NAMES, SIMPLE_FIELD_TYPES);
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
