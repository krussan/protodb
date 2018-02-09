package se.qxx.protodb.test;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import se.qxx.protodb.ProtoDB;
import se.qxx.protodb.ProtoDBFactory;
import se.qxx.protodb.exceptions.DatabaseNotSupportedException;

@RunWith(Parameterized.class)
public class TestSelect extends TestBase {
	ProtoDB db = null;
	
	@Parameters
    public static Collection<Object[]> data() {
    	return getParams("testParamsFile");
    }
    
    public TestSelect(String driver, String connectionString) throws DatabaseNotSupportedException {
    	db = ProtoDBFactory.getInstance(driver, connectionString);
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
