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

import se.qxx.protodb.ProtoDB;
import se.qxx.protodb.ProtoDBFactory;
import se.qxx.protodb.exceptions.DatabaseNotSupportedException;
import se.qxx.protodb.exceptions.IDFieldNotFoundException;
import se.qxx.protodb.exceptions.SearchFieldNotFoundException;
import se.qxx.protodb.test.TestDomain.ObjectOne;
import se.qxx.protodb.test.TestDomain.ObjectTwo;
import se.qxx.protodb.test.TestDomain.SimpleTest;

@RunWith(Parameterized.class)
public class TestExcludingObjects extends TestBase {

	ProtoDB db = null;

	@Parameters
    public static Collection<Object[]> data() {
    	return getParams();
    }
    
    public TestExcludingObjects(String driver, String connectionString) throws DatabaseNotSupportedException, ClassNotFoundException, SQLException {
    	db = ProtoDBFactory.getInstance(driver, connectionString);
    	
    	if (ProtoDBFactory.isSqlite(driver)) {
    		File f = new File(connectionString);
    			f.delete();
    	}

    	
    	if (ProtoDBFactory.isMySql(driver)) {
    		db.dropAllTables();
    	}

    }

	
	@Before
	public void Setup() throws ClassNotFoundException, SQLException, IDFieldNotFoundException {		
	    db.setupDatabase(TestDomain.ObjectTwo.newBuilder());
	    
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
	}
	
	@Test
	public void TestExcluding() {	
		try {
			List<String> excludedObjects = new ArrayList<String>();
			excludedObjects.add("testOne");
			
			List<TestDomain.ObjectOne> result =
				db.find(
					TestDomain.ObjectOne.getDefaultInstance(), 
					"testOne.bb", 
					true, 
					false,
					excludedObjects);
			
			// we should get one single result..
			assertEquals(1, result.size());
		
			TestDomain.ObjectOne b = result.get(0);
			assertEquals(b.getOois(), 986);
			
			TestDomain.SimpleTest o1 = b.getTestOne();
			assertFalse(o1.isInitialized());
			
		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException e) {
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
			
			TestDomain.SimpleTest o2testOne = result.getTestOne();
			assertFalse(o2testOne.isInitialized());
			
			TestDomain.ObjectOne o2TestTwo = result.getTestTwo();
			assertTrue(o2TestTwo.isInitialized());
			
			assertEquals(986, o2TestTwo.getOois());
			
			TestDomain.SimpleTest o1TestOne = o2TestTwo.getTestOne();
			assertFalse(o1TestOne.isInitialized());
			
			
			
		} catch (SQLException | ClassNotFoundException e) {
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
	
}
