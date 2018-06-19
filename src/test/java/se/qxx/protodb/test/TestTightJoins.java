package se.qxx.protodb.test;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.protobuf.ByteString;

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
import se.qxx.protodb.model.ProtoDBSearchOperator;
import se.qxx.protodb.test.TestDomain.ObjectOne;
import se.qxx.protodb.test.TestDomain.ObjectThree;
import se.qxx.protodb.test.TestDomain.ObjectTwo;
import se.qxx.protodb.test.TestDomain.RepObjectOne;
import se.qxx.protodb.test.TestDomain.RepObjectTwo;
import se.qxx.protodb.test.TestDomain.SimpleTest;
import se.qxx.protodb.test.TestDomain.SimpleTwo;

@RunWith(Parameterized.class)
public class TestTightJoins extends TestBase {

	ProtoDB db = null;

	@Parameters
    public static Collection<Object[]> data() {
    	return getParams("testParamsFile");
    }
    
    public TestTightJoins(String driver, String connectionString) throws DatabaseNotSupportedException, ClassNotFoundException, SQLException {
    	db = ProtoDBFactory.getInstance(driver, connectionString);
    	
    	clearDatabase(db, connectionString);
    }

	@Before
	public void Setup() throws ClassNotFoundException, SQLException, IDFieldNotFoundException {
		
	    db.setupDatabase(TestDomain.ObjectThree.newBuilder());
	    
	    TestDomain.SimpleTest t = TestDomain.SimpleTest.newBuilder()
				.setID(-1)
				.setBb(false)
				.setBy(ByteString.copyFrom(new byte[] {5,8,6}))
				.setDd(1467802579378.62352352)
				.setFf((float) 555444333.213)
				.setIl(999999998)
				.setIs(999999998)
				.setSs("ThisIsATest")
				.build();
		
		TestDomain.SimpleTest t2 = TestDomain.SimpleTest.newBuilder()
				.setID(-1)
				.setBb(true)
				.setSs("ThisIsAnotherTest")
				.build();		
		
		t2 = db.save(t2);
		
		TestDomain.ObjectOne o1 = TestDomain.ObjectOne.newBuilder()
				.setID(-1)
				.setOois(986)
				.setTestOne(TestDomain.SimpleTest.newBuilder()
						.setID(-1)
						.setBb(false)
						.setBy(ByteString.copyFrom(new byte[] {5,8,6}))
						.setDd(1467802579378.62352352)
						.setFf((float) 555444333.213)
						.setIl(999999998)
						.setIs(999999998)
						.setSs("ThisIsATestOfObjectOne")
				).build();
		
		TestDomain.ObjectOne o1a = TestDomain.ObjectOne.newBuilder()
				.setID(-1)
				.setOois(986)
				.setTestOne(TestDomain.SimpleTest.newBuilder()
						.setID(-1)
						.setBb(false)
						.setBy(ByteString.copyFrom(new byte[] {5,8,6}))
						.setDd(1467802579378.62352352)
						.setFf((float) 555444333.213)
						.setIl(999999998)
						.setIs(999999998)
						.setSs("ObjectOneA")
				).build();
		
		
	    TestDomain.ObjectTwo o2 = 
	    		TestDomain.ObjectTwo.newBuilder()
	    			.setID(-1)
	    			.setOtis(666)
	    			.setTestOne(t)
	    			.setTestTwo(o1)
	    			.build();
	    	
	    TestDomain.ObjectThree o3 = TestDomain.ObjectThree.newBuilder()
	    		.setApa(t2)
	    		.setBepa(o2)
	    		.setID(-1)
	    		.build();
	    
	    db.save(o3);
	    

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
	public void TestSearchRecursive() {
		try {
			List<ObjectThree> list = 
				db.search(
					SearchOptions.newBuilder(ObjectThree.getDefaultInstance())
						.addFieldName("bepa.testtwo.testone.ss")
						.addOperator(ProtoDBSearchOperator.Equals)
						.addSearchArgument("ThisIsATestOfObjectOne"));

			assertEquals(1, list.size());
			
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void TestRepeatedRepeated() {
		try {
			RepObjectTwo two = RepObjectTwo.newBuilder()
					.setID(10)
					.setTitle("TestRepRep")
					.build();
			
			ProtoDBScanner scanner = new ProtoDBScanner(two, db.getDatabaseBackend());
			JoinResult result = Searcher.getJoinQuery(scanner, false, true);
			result.addWhereClause(scanner, "listRepObject.list_of_objects.title", "title", ProtoDBSearchOperator.Equals);
			String expected = String.format(
					"SELECT DISTINCT A.%1$sID%2$s AS A_ID, A.%1$stitle%2$s AS A_title FROM "
					+ "RepObjectTwo AS A "
					+ "LEFT JOIN RepObjectTwoRepObjectOne_ListRepObject AS L1 "
					+ "ON L1._repobjecttwo_ID = A.ID "
					+ "LEFT JOIN RepObjectOne AS AA "
					+ "ON L1._repobjectone_ID = AA.ID "
					+ "LEFT JOIN RepObjectOneSimpleTwo_Listofobjects AS L2 "
					+ "ON L2._repobjectone_ID = AA.ID "
					+ "LEFT JOIN SimpleTwo AS AAA "
					+ "ON L2._simpletwo_ID = AAA.ID "
					+ "WHERE AAA.title = ? ",
					db.getDatabaseBackend().getStartBracket(),
					db.getDatabaseBackend().getEndBracket());
			
			String actual = result.getSql();
			
			assertEquals(expected, actual);
			
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	public void TestRepeatedRepeatedSearch() {
		try {

			RepObjectOne rep_o1 = RepObjectOne.newBuilder()
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
		    
			RepObjectTwo two = RepObjectTwo.newBuilder()
					.setID(20)
					.setTitle("TestRepRep")
					.addListRepObject(rep_o1)
					.build();
			
			db.save(two);
			
			
			List<RepObjectTwo> result = 
				db.search(
					SearchOptions.newBuilder(two)
						.addFieldName("title")
						.addOperator(ProtoDBSearchOperator.Equals)
						.addSearchArgument("TestRepRep")
						.setShallow(false));
			
			assertNotNull(result);
			assertEquals(1, result.size());
			
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}


}
