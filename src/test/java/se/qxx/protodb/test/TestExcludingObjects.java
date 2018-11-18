package se.qxx.protodb.test;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

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
import se.qxx.protodb.exceptions.SearchOptionsNotInitializedException;
import se.qxx.protodb.model.ProtoDBSearchOperator;
import se.qxx.protodb.test.TestDomain.ObjectOne;
import se.qxx.protodb.test.TestDomain.ObjectTwo;
import se.qxx.protodb.test.TestDomain.RepObjectOne;
import se.qxx.protodb.test.TestDomain.RepObjectThree;
import se.qxx.protodb.test.TestDomain.SimpleTest;

@RunWith(Parameterized.class)
public class TestExcludingObjects extends TestBase {

	ProtoDB db = null;

	@Parameters
    public static Collection<Object[]> data() {
    	return getParams("testParamsFile");
    }
    
    public TestExcludingObjects(String driver, String connectionString) throws DatabaseNotSupportedException, ClassNotFoundException, SQLException {
    	db = ProtoDBFactory.getInstance(driver, connectionString);
    	
    	clearDatabase(db, connectionString);
    }

	
	@Before
	public void Setup() throws ClassNotFoundException, SQLException, IDFieldNotFoundException {		
	    db.setupDatabase(TestDomain.ObjectTwo.newBuilder());
	    db.setupDatabase(TestDomain.RepObjectOne.newBuilder());
	    db.setupDatabase(TestDomain.RepObjectThree.newBuilder());
	    
	    ObjectTwo o2 = db.get(1, TestDomain.ObjectTwo.getDefaultInstance());
	    
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
		
		TestDomain.SimpleTest tt = TestDomain.SimpleTest.newBuilder()
				.setID(-1)
				.setBb(false)
				.setBy(ByteString.copyFrom(new byte[] {7, 8, 9}))
				.setDd(1467802579378.62352352)
				.setFf((float) 555444333.213)
				.setIl(999999998)
				.setIs(999999998)
				.setSs("ABCDEFGHIJK")
				.build();
		
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
		
		
	    if (o2 == null) {
	    	o2 = 
	    		TestDomain.ObjectTwo.newBuilder()
	    			.setID(-1)
	    			.setOtis(666)
	    			.setTestOne(t)
	    			.setTestTwo(o1)
	    			.build();
	    	
	    	db.save(o2);
	    }
	    
	    TestDomain.SimpleTwo t1 = TestDomain.SimpleTwo.newBuilder()
	    	.setID(-1)
	    	.setTitle("thisIsATitle")
	    	.setDirector("madeByThisDirector")
	    	.build();
	    
	    TestDomain.SimpleTwo t2 = TestDomain.SimpleTwo.newBuilder()
	    		.setID(-1)
		    	.setTitle("thisIsAlsoATitle")
		    	.setDirector("madeByAnotherDirector")
		    	.build();
	    
	    TestDomain.RepObjectOne r1 = TestDomain.RepObjectOne.newBuilder()
	    		.setID(-1)
	    		.setHappycamper(3)
	    		.addListOfObjects(t1)
	    		.addListOfObjects(t2)
	    		.build();
	    
	    db.save(r1);
	    
	    TestDomain.RepObjectThree r3 = TestDomain.RepObjectThree.newBuilder()
	    		.setID(-1)
	    		.setTitle("TitleA")
	    		.addListObjects(t)
	    		.addListObjects(tt)
	    		.build();

	    TestDomain.RepObjectThree r31 = TestDomain.RepObjectThree.newBuilder()
	    		.setID(-1)
	    		.setTitle("TitleB")
	    		.addListObjects(t)
	    		.addListObjects(tt)
	    		.build();

	    db.save(r3);
	    db.save(r31);
	    
	}
	
	@Test
	public void TestExcluding() {	
		try {
			List<String> excludedObjects = new ArrayList<String>();
			excludedObjects.add("testOne");
			
			List<TestDomain.ObjectOne> result =
				db.find(
					TestDomain.ObjectOne.getDefaultInstance(), 
					"testOne.ss", 
					"ThisIsATestOfObjectOne", 
					false,
					excludedObjects);
			
			// we should get one single result..
			assertEquals(1, result.size());
		
			TestDomain.ObjectOne b = result.get(0);
			assertEquals(b.getOois(), 986);
			assertFalse(b.hasTestOne());
			
			
		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void TestExcludingSearch() {	
		try {

			List<TestDomain.ObjectOne> result =
				db.search(
					SearchOptions.newBuilder(TestDomain.ObjectOne.getDefaultInstance())
					.addFieldName("testOne.ss")
					.addSearchArgument("ThisIsATestOfObjectOne")
					.addOperator(ProtoDBSearchOperator.Equals)
					.addExcludedObject("testOne"));
			
			// we should get one single result..
			assertEquals(1, result.size());
		
			TestDomain.ObjectOne b = result.get(0);
			assertEquals(b.getOois(), 986);
			assertFalse(b.hasTestOne());
			
		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException | ProtoDBParserException | SearchOptionsNotInitializedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void TestExcludingOnObjectTwo() {	
		try {
			List<String> excludedObjects = new ArrayList<String>();
			excludedObjects.add("testTwo.testOne");
			excludedObjects.add("testOne");
			
			ObjectTwo result =
				db.get(1, excludedObjects, TestDomain.ObjectTwo.getDefaultInstance());

			assertNotNull(result);
			assertEquals(666, result.getOtis());
			assertFalse(result.hasTestOne());
			assertTrue(result.hasTestTwo());
			
			TestDomain.ObjectOne o2TestTwo = result.getTestTwo();
			assertEquals(986, o2TestTwo.getOois());
			assertFalse(o2TestTwo.hasTestOne());
			
			
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void TestExcludingOnObjectTwoSearch() {	
		try {
			List<String> excludedObjects = new ArrayList<String>();
			excludedObjects.add("testTwo.testOne");
			excludedObjects.add("testOne");
			
			List<ObjectTwo> result = db.search(
					SearchOptions.newBuilder(TestDomain.ObjectTwo.getDefaultInstance())
					.addFieldName("ID")
					.addSearchArgument(1)
					.addOperator(ProtoDBSearchOperator.Equals)
					.addExcludedObject("testTwo.testOne")
					.addExcludedObject("testOne"));

			assertNotNull(result);
			assertEquals(1, result.size());
			
			assertEquals(666, result.get(0).getOtis());
			
			assertFalse(result.get(0).hasTestOne());
			
			assertTrue(result.get(0).hasTestTwo());
			TestDomain.ObjectOne o2TestTwo = result.get(0).getTestTwo();
			assertTrue(o2TestTwo.isInitialized());
			
			assertEquals(986, o2TestTwo.getOois());
			assertFalse(o2TestTwo.hasTestOne());
			
			
		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException | ProtoDBParserException | SearchOptionsNotInitializedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void TestExcludeOnRepeatedObjects() {
		try {
			List<String> excludedObjects = new ArrayList<String>();
			excludedObjects.add("list_of_objects");
			
			List<TestDomain.RepObjectOne> result =
					db.find(
						TestDomain.RepObjectOne.getDefaultInstance(), 
						"happycamper", 
						3, 
						false,
						excludedObjects);
			
			assertEquals(1, result.size());
			assertEquals(0, result.get(0).getListOfObjectsCount());

		} catch (ClassNotFoundException | SQLException | SearchFieldNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	@Test
	public void TestExcludingBlobSearch() {
		try {
			
			List<TestDomain.ObjectOne> result =
				db.search(
					SearchOptions.newBuilder(TestDomain.ObjectOne.getDefaultInstance())
						.addFieldName("testOne.ss")
						.addSearchArgument("ThisIsATestOfObjectOne")
						.addOperator(ProtoDBSearchOperator.Equals)
						.addExcludedObject("testOne.by"));
				
			// we should get one single result..
			assertEquals(1, result.size());
		
			TestDomain.ObjectOne b = result.get(0);
			assertEquals(b.getOois(), 986);
			
			TestDomain.SimpleTest o1 = b.getTestOne();
			ByteString data = o1.getBy();
			assertTrue(data.isEmpty());
			
		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException | ProtoDBParserException | SearchOptionsNotInitializedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		
	}
	

	@Test
	public void TestExcludingBlobSearchSQL() {
		ProtoDBScanner scanner = new ProtoDBScanner(TestDomain.ObjectOne.getDefaultInstance(), db.getDatabaseBackend());
		List<String> excludedObjects = new ArrayList<String>();
		excludedObjects.add("testOne.by");
		
		JoinResult result = Searcher.getJoinQuery(scanner, true, true, -1, -1, excludedObjects);
		
		String expected = 
				String.format(
					"SELECT "
					+ "A.%1$sID%2$s AS A_ID, "
					+ "A.%1$soois%2$s AS A_oois, "
					+ "AA.%1$sID%2$s AS AA_ID, "
					+ "AA.%1$sdd%2$s AS AA_dd, "
					+ "AA.%1$sff%2$s AS AA_ff, "
					+ "AA.%1$sis%2$s AS AA_is, "
					+ "AA.%1$sil%2$s AS AA_il, "
					+ "AA.%1$sbb%2$s AS AA_bb, "
					+ "AA.%1$sss%2$s AS AA_ss "
					+ "FROM ObjectOne AS A "
					+ "LEFT JOIN SimpleTest AS AA "
					+ "ON A._testone_ID = AA.ID ",
					db.getDatabaseBackend().getStartBracket(),
					db.getDatabaseBackend().getEndBracket());
			
			assertEquals(expected, result.getSql());
		
	}

	@Test
	public void TestExcludeOnSubObjectsSearch() {
		try {
			List<RepObjectOne> result = db.search(
				SearchOptions.newBuilder(TestDomain.RepObjectOne.getDefaultInstance())
					.addFieldName("ID")
					.addSearchArgument(1)
					.addOperator(ProtoDBSearchOperator.Equals)
					.addExcludedObject("list_of_objects"));
			
			assertNotNull(result);
			assertEquals(1, result.size());
			
			assertEquals(0, result.get(0).getListOfObjectsCount());

		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException | ProtoDBParserException | SearchOptionsNotInitializedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail(e.getMessage());
		}

	}

	@Test
	public void TestExcludeBlobsOnSubObjectsSearch() {
		try {

			List<RepObjectThree> result = db.search(
				SearchOptions.newBuilder(TestDomain.RepObjectThree.getDefaultInstance())
					.addFieldName("title")
					.addSearchArgument("%")
					.addOperator(ProtoDBSearchOperator.Like)
					.addExcludedObject("listobjects.by"));
			
			assertNotNull(result);
			assertEquals(2, result.size());
			
			assertEquals(0, result.get(0).getListObjects(0).getBy().size());
			assertEquals(0, result.get(0).getListObjects(1).getBy().size());
			assertEquals(0, result.get(1).getListObjects(0).getBy().size());
			assertEquals(0, result.get(1).getListObjects(1).getBy().size());

		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException | ProtoDBParserException | SearchOptionsNotInitializedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail(e.getMessage());
		}

	}

}
