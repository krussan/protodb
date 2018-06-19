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

import se.qxx.protodb.JoinResult;
import se.qxx.protodb.ProtoDB;
import se.qxx.protodb.ProtoDBFactory;
import se.qxx.protodb.ProtoDBScanner;
import se.qxx.protodb.SearchOptions;
import se.qxx.protodb.Searcher;
import se.qxx.protodb.exceptions.DatabaseNotSupportedException;
import se.qxx.protodb.exceptions.IDFieldNotFoundException;
import se.qxx.protodb.exceptions.ProtoDBParserException;
import se.qxx.protodb.exceptions.SearchFieldNotFoundException;
import se.qxx.protodb.exceptions.SearchOptionsNotInitializedException;
import se.qxx.protodb.model.ProtoDBSearchOperator;
import se.qxx.protodb.test.TestDomain.RepObjectOne;
import se.qxx.protodb.test.TestDomain.SimpleTwo;

@RunWith(Parameterized.class)
public class TestSearchRepeated extends TestBase {
	ProtoDB db = null;
	
	@Parameters
    public static Collection<Object[]> data() {
    	return getParams("testParamsFile");
    }
    
    public TestSearchRepeated(String driver, String connectionString) throws DatabaseNotSupportedException, ClassNotFoundException, SQLException {
    	db = ProtoDBFactory.getInstance(driver, connectionString);
    	
    	clearDatabase(db, connectionString);
	}	

	@Before
	public void Setup() throws ClassNotFoundException, SQLException, IDFieldNotFoundException {
		
	    db.setupDatabase(TestDomain.RepObjectOne.newBuilder());
		
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

		RepObjectOne o2 = RepObjectOne.newBuilder()
				.setID(11)
				.setHappycamper(444)
				.addListOfObjects(SimpleTwo.newBuilder()
						.setID(3)
						.setDirector("direction")
						.setTitle("up_side")
						.build())
				.build();

		db.save(o1);
		db.save(o2);
	}
	
	@Test
	public void TestSearchExactByJoin() {	
		try {
			List<TestDomain.RepObjectOne> result =
				db.search(
					SearchOptions.newBuilder(TestDomain.RepObjectOne.getDefaultInstance())
					 .addFieldName("list_of_objects.title")
					 .addSearchArgument("who_said_that")
					 .addOperator(ProtoDBSearchOperator.Equals));
			
			
			// we should get one single result..
			assertEquals(1, result.size());
			
			// we should get three sub results
			assertEquals(2, result.get(0).getListOfObjectsList().size());

		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException | ProtoDBParserException | SearchOptionsNotInitializedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void TestSearchJoinQuery() {
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
			JoinResult result = Searcher.getJoinQuery(scanner, false, true);

			// the query of the repeated subobjects need to be populated separately
			String expected =
				String.format(
					"SELECT DISTINCT "
					+ "A.%1$sID%2$s AS A_ID, "
					+ "A.%1$shappycamper%2$s AS A_happycamper "
					+ "FROM RepObjectOne AS A ",
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
	public void TestShallowCopy() {
		try {
			List<TestDomain.RepObjectOne> result =
				db.search(
					SearchOptions.newBuilder(TestDomain.RepObjectOne.getDefaultInstance())
					.addSearchArgument("%")
					.addOperator(ProtoDBSearchOperator.Like)
					.setShallow(true));
			
			assertNotNull(result);
			
			// we should get one single result..
			assertEquals(2, result.size());
			
			// we should get three sub results
			assertEquals(0, result.get(0).getListOfObjectsList().size());

		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException | ProtoDBParserException | SearchOptionsNotInitializedException  e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void TestSearchNoDuplicates() {	
		try {
			List<TestDomain.RepObjectOne> result =
				db.search(
					SearchOptions.newBuilder(TestDomain.RepObjectOne.getDefaultInstance())
					.addSearchArgument("%")
					.addOperator(ProtoDBSearchOperator.Like));
			
			// we should get two single result and not three as the join will create duplicates
			// of the parent item. This is not wanted.
			assertEquals(2, result.size());

		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException | ProtoDBParserException | SearchOptionsNotInitializedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
}
