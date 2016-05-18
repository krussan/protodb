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

	
	@Test
	public void TestSearchLike() {	
		try {
			List<TestDomain.SimpleTwo> result =
				db.find(
					TestDomain.SimpleTwo.getDefaultInstance(), 
					"director", 
					"madeBy%", 
					true);
			
			// we should get one single result..
			assertEquals(2, result.size());
		
			TestDomain.SimpleTwo o1 = result.get(0);
			assertEquals("thisIsATitle", o1.getTitle());
			assertEquals("madeByThisDirector", o1.getDirector());
			
			TestDomain.SimpleTwo o2 = result.get(1);
			assertEquals("thisIsAlsoATitle", o2.getTitle());
			assertEquals("madeByAnotherDirector", o2.getDirector());
			
//			PreparedStatement prep = "SELECT * FROM SimpleTest";
//			
//			testTableStructure(db, "SimpleTest", SIMPLE_FIELD_NAMES, SIMPLE_FIELD_TYPES);
		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException  e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void TestSearchObjectExactBoolean() {	
		try {
			//TODO: WHERE clause on Boolean does not appear to work?
			List<TestDomain.ObjectOne> result =
				db.find(
					TestDomain.ObjectOne.getDefaultInstance(), 
					"testOne.bb", 
					true, 
					false);
			
			// we should get one single result..
			assertEquals(1, result.size());
		
			TestDomain.ObjectOne b = result.get(0);
			assertEquals(b.getOois(), 986);
			
			TestDomain.SimpleTest o1 = b.getTestOne();
			assertEquals(o1.getBb(), true);
			assertEquals(o1.getDd(), 1467802579378.62, 0.0);
			assertEquals(o1.getFf(), (float)555444333.213, 0.0);
			assertEquals(o1.getIs(), 999999998);
			assertEquals(o1.getIl(), 999999998);
			assertEquals(o1.getSs(), "ThisIsATrueTest");
			
//			PreparedStatement prep = "SELECT * FROM SimpleTest";
//			
//			testTableStructure(db, "SimpleTest", SIMPLE_FIELD_NAMES, SIMPLE_FIELD_TYPES);
		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException  e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void TestSearchRepeatedObjects() {	
		try {
			List<TestDomain.RepObjectOne> result =
				db.find(
					TestDomain.RepObjectOne.getDefaultInstance(), 
					"list_of_objects.director", 
					"madeByThisDirector", 
					false);
			
			// we should get one single result..
			assertEquals(1, result.size());
		
			TestDomain.RepObjectOne b = result.get(0);
			assertEquals(b.getHappycamper(), 3);
			
		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException  e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}	
	
}
