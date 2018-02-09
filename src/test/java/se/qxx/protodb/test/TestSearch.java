package se.qxx.protodb.test;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import se.qxx.protodb.ProtoDB;
import se.qxx.protodb.ProtoDBFactory;
import se.qxx.protodb.exceptions.DatabaseNotSupportedException;
import se.qxx.protodb.exceptions.SearchFieldNotFoundException;

@RunWith(Parameterized.class)
public class TestSearch extends TestBase {
	ProtoDB db = null;
	
	@Parameters
	public static Collection<Object[]> data() {
		return getParams("selectParamsFile");
    }
    
    public TestSearch(String driver, String connectionString) throws DatabaseNotSupportedException, ClassNotFoundException, SQLException {
    	db = ProtoDBFactory.getInstance(driver, connectionString);
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
