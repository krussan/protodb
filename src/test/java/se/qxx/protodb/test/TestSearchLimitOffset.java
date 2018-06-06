package se.qxx.protodb.test;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import se.qxx.protodb.JoinResult;
import se.qxx.protodb.ProtoDB;
import se.qxx.protodb.ProtoDBFactory;
import se.qxx.protodb.ProtoDBScanner;
import se.qxx.protodb.ProtoDBSort;
import se.qxx.protodb.Searcher;
import se.qxx.protodb.exceptions.DatabaseNotSupportedException;
import se.qxx.protodb.exceptions.IDFieldNotFoundException;
import se.qxx.protodb.exceptions.ProtoDBParserException;
import se.qxx.protodb.exceptions.SearchFieldNotFoundException;
import se.qxx.protodb.model.ProtoDBSearchOperator;
import se.qxx.protodb.test.TestDomain.RepObjectOne;
import se.qxx.protodb.test.TestDomain.SimpleTwo;

@RunWith(Parameterized.class)
public class TestSearchLimitOffset extends TestBase {
	ProtoDB db = null;
	
	@Parameters
    public static Collection<Object[]> data() {
    	return getParams("testParamsFile");
    }
    
    public TestSearchLimitOffset(String driver, String connectionString) throws DatabaseNotSupportedException, ClassNotFoundException, SQLException {
    	db = ProtoDBFactory.getInstance(driver, connectionString);
    	
    	clearDatabase(db, connectionString);
    }	
	
	@Test
	public void TestLimitOffset() throws IDFieldNotFoundException {
		try {
			
		    db.setupDatabase(TestDomain.RepObjectOne.newBuilder());
			
		    for (int i=1;i<50;i++) {
				RepObjectOne o1 = RepObjectOne.newBuilder()
						.setID(i)
						.setHappycamper(i)
						.build();
				
				db.save(o1);
		    }

			List<TestDomain.RepObjectOne> result =
				db.search(
					TestDomain.RepObjectOne.getDefaultInstance(), 
					"", 
					"%", 
					ProtoDBSearchOperator.Like,
					10, 0);
			
			// we should get 10 results
			assertEquals(10, result.size());
			
			// the first should be number 1
			assertEquals(1, result.get(0).getHappycamper());
			
			// the last should be number 10
			assertEquals(10, result.get(9).getHappycamper());

			
			result = db.search(
						TestDomain.RepObjectOne.getDefaultInstance(), 
						"", 
						"%", 
						ProtoDBSearchOperator.Like,
						10, 30);


			// we should get 10 results
			assertEquals(10, result.size());
			
			// the first should be number 31
			assertEquals(31, result.get(0).getHappycamper());
			
			// the last should be number 40
			assertEquals(40, result.get(9).getHappycamper());

		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException | ProtoDBParserException  e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
	}
	
	@Test
	public void TestLimitOffsetQuery() {
		try {
			
			RepObjectOne o1 = RepObjectOne.newBuilder()
					.setID(10)
					.setHappycamper(555)
					.addListOfObjects(SimpleTwo.newBuilder()
							.setID(1)
							.setDirector("directThis")
							.setTitle("thisisatitle")
							.build())
					.addListOfObjects(SimpleTwo.newBuilder()
							.setID(2)
							.setDirector("no_way")
							.setTitle("who_said_that")
							.build())
					.build();
			
			ProtoDBScanner scanner = new ProtoDBScanner(o1, db.getDatabaseBackend());
			JoinResult result = Searcher.getJoinQuery(scanner, false, true, 10, 1);

			// the query of the repeated subobjects need to be populated separately
			String expected = 
				String.format(
					"SELECT DISTINCT "
					+ "A.%1$sID%2$s AS A_ID, "
					+ "A.%1$shappycamper%2$s AS A_happycamper "
					+ "FROM   RepObjectOne AS A   "
					+ "  LIMIT 10 "
					+ "OFFSET 1",
					db.getDatabaseBackend().getStartBracket(),
					db.getDatabaseBackend().getEndBracket());
			
			assertEquals(expected, result.getSql());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void TestLimitOffsetOrderBy() throws IDFieldNotFoundException {
		try {
			
		    db.setupDatabase(TestDomain.RepObjectOne.newBuilder());
			
		    for (int i=1;i<50;i++) {
				RepObjectOne o1 = RepObjectOne.newBuilder()
						.setID(i)
						.setHappycamper(i)
						.build();
				
				db.save(o1);
		    }

			List<TestDomain.RepObjectOne> result =
				db.search(
					TestDomain.RepObjectOne.getDefaultInstance(), 
					"", 
					"%", 
					ProtoDBSearchOperator.Like,
					10, 
					0,
					"happycamper",
					ProtoDBSort.Desc);
			
			// we should get 10 results
			assertEquals(10, result.size());
			
			// the first should be number 1
			assertEquals(49, result.get(0).getHappycamper());
			
			// the last should be number 10
			assertEquals(40, result.get(9).getHappycamper());

		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException | ProtoDBParserException  e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
	}

}
