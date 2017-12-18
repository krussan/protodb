package se.qxx.protodb.test;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.SQLException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import se.qxx.protodb.JoinResult;
import se.qxx.protodb.ProtoDB;
import se.qxx.protodb.ProtoDBScanner;
import se.qxx.protodb.Searcher;
import se.qxx.protodb.exceptions.IDFieldNotFoundException;
import se.qxx.protodb.exceptions.SearchFieldNotFoundException;
import se.qxx.protodb.model.ProtoDBSearchOperator;
import se.qxx.protodb.test.TestDomain.RepObjectOne;
import se.qxx.protodb.test.TestDomain.SimpleTwo;

public class TestSearchLimitOffset {
	ProtoDB db = null;
	
	private final String DATABASE_FILE = "protodb_limit_offset_test.db";

	@Before
	public void Setup() throws ClassNotFoundException, SQLException, IDFieldNotFoundException {
		
		File f = new File(DATABASE_FILE);
		if (f.exists())
			f.delete();
		
	    db = new ProtoDB(DATABASE_FILE);
	    
	
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

		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException  e) {
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
			
			ProtoDBScanner scanner = new ProtoDBScanner(o1);
			JoinResult result = Searcher.getJoinQuery(scanner, false, true, 10, 1);

			// the query of the repeated subobjects need to be populated separately
			String expected = "SELECT DISTINCT "
					+ "A.[ID] AS A_ID, "
					+ "A.[happycamper] AS A_happycamper "
					+ "FROM   RepObjectOne AS A   "
					+ "LEFT JOIN RepObjectOneSimpleTwo_Listofobjects AS L1 "
					+ " ON L1._repobjectone_ID = A.ID "
					+ "LEFT JOIN SimpleTwo AS AA "
					+ " ON L1._simpletwo_ID = AA.ID "
					+ " LIMIT 10 "
					+ "OFFSET 1";
			
			assertEquals(expected, result.getJoinClause());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
