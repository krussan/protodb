package se.qxx.protodb.test;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.SQLException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.ByteString;

import se.qxx.protodb.ProtoDB;
import se.qxx.protodb.ProtoDBScanner;
import se.qxx.protodb.exceptions.IDFieldNotFoundException;
import se.qxx.protodb.exceptions.SearchFieldNotFoundException;
import se.qxx.protodb.test.TestDomain.ObjectTwo;

public class TestSearchRecursive {
	ProtoDB db = null;
	
	private final String DATABASE_FILE = "protodb_search_test.db";
	
	@Before
	public void Setup() throws ClassNotFoundException, SQLException, IDFieldNotFoundException {
		File f = new File(DATABASE_FILE);
		if (f.exists())
			f.delete();
		
	    db = new ProtoDB(DATABASE_FILE);
	    
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
	public void TestSearchExact() {	
		try {
			List<TestDomain.ObjectThree> result =
				db.find(
					TestDomain.ObjectThree.getDefaultInstance(), 
					"bepa.testTwo.testOne.ss", 
					"ThisIsATestOfObjectOne", 
					false);
			
			// we should get one single result..
			assertEquals(1, result.size());
		
			TestDomain.ObjectThree b = result.get(0);

			assertEquals("ThisIsAnotherTest", b.getApa().getSs());
			assertEquals("ThisIsATest", b.getBepa().getTestOne().getSs());
			assertEquals("ThisIsATestOfObjectOne", b.getBepa().getTestTwo().getTestOne().getSs());
			assertEquals(666, b.getBepa().getOtis());
			
//			PreparedStatement prep = "SELECT * FROM SimpleTest";
//			
//			testTableStructure(db, "SimpleTest", SIMPLE_FIELD_NAMES, SIMPLE_FIELD_TYPES);
		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException  e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void TestSearchJoinQuery() {
	    TestDomain.SimpleTest t = TestDomain.SimpleTest.newBuilder()
				.setID(1)
				.setBb(false)
				.setBy(ByteString.copyFrom(new byte[] {5,8,6}))
				.setDd(1467802579378.62352352)
				.setFf((float) 555444333.213)
				.setIl(999999998)
				.setIs(999999998)
				.setSs("ThisIsATest")
				.build();
		
		TestDomain.SimpleTest t2 = TestDomain.SimpleTest.newBuilder()
				.setID(2)
				.setBb(true)
				.setSs("ThisIsAnotherTest")
				.build();		
		
		TestDomain.ObjectOne o1 = TestDomain.ObjectOne.newBuilder()
				.setID(3)
				.setOois(986)
				.setTestOne(TestDomain.SimpleTest.newBuilder()
						.setID(6)
						.setBb(false)
						.setBy(ByteString.copyFrom(new byte[] {5,8,6}))
						.setDd(1467802579378.62352352)
						.setFf((float) 555444333.213)
						.setIl(999999998)
						.setIs(999999998)
						.setSs("ThisIsATestOfObjectOne")
				).build();
		
		
	    TestDomain.ObjectTwo o2 = 
	    		TestDomain.ObjectTwo.newBuilder()
	    			.setID(4)
	    			.setOtis(666)
	    			.setTestOne(t)
	    			.setTestTwo(o1)
	    			.build();
	    	
	    TestDomain.ObjectThree o3 = TestDomain.ObjectThree.newBuilder()
	    		.setApa(t2)
	    		.setBepa(o2)
	    		.setID(5)
	    		.build();
	    		
		ProtoDBScanner scanner = new ProtoDBScanner(o3);
		String actual = scanner.getJoinQuery(false);
		
		String expected = "SELECT "
				+ "A.ID AS A_ID, "
				+ "AA.ID AS AA_ID, "
				+ "AA.dd AS AA_dd, "
				+ "AA.ff AS AA_ff, "
				+ "AA.is AS AA_is, "
				+ "AA.il AS AA_il, "
				+ "AA.bb AS AA_bb, "
				+ "AA.ss AS AA_ss, "
				//+ "AA.by AS AA_by, "
				+ "AB.ID AS AB_ID, "
				+ "AB.otis AS AB_otis, "
				+ "ABA.ID AS ABA_ID, "
				+ "ABA.dd AS ABA_dd, "
				+ "ABA.ff AS ABA_ff, "
				+ "ABA.is AS ABA_is, "
				+ "ABA.il AS ABA_il, "
				+ "ABA.bb AS ABA_bb, "
				+ "ABA.ss AS ABA_ss, "
//				+ "ABA.by AS ABA_by, "
				+ "ABB.ID AS ABB_ID, "
				+ "ABB.oois AS ABB_oois, "
				+ "ABBA.ID AS ABBA_ID, "
				+ "ABBA.dd AS ABBA_dd, "
				+ "ABBA.ff AS ABBA_ff, "
				+ "ABBA.is AS ABBA_is, "
				+ "ABBA.il AS ABBA_il, "
				+ "ABBA.bb AS ABBA_bb, "
				+ "ABBA.ss AS ABBA_ss, "
//				+ "ABBA.by AS ABBA_by "
				+ "FROM ObjectThree AS A "
				+ "LEFT/INNER JOIN ObjectThreeSimpleTest_SimpleTest AS L1 "
				+ " ON L1._objectThree_ID = A.ID "
				+ "LEFT/INNER JOIN SimpleTest AS AA "
				+ " ON L1._simpleTest_ID = AA.ID "
				+ "LEFT/INNER JOIN ObjectThreeObjectTwo_ObjectTwo AS L2 "
				+ " ON L2._objectThree_ID = A.ID "
				+ "LEFT/INNER JOIN ObjectTwo AS AB "
				+ " ON L2._objectTwo_ID = AB.ID "
				+ "LEFT/INNER JOIN ObjectTwoSimpleTest_SimpleTest AS L3 "
				+ " ON L3._objectTwo_ID = AB.ID "
				+ "LEFT/INNER JOIN SimpleTest AS ABA "
				+ " ON L3._simpleTest_ID = ABA.ID "
				+ "LEFT/INNER JOIN ObjectTwoObjectOne_ObjectOne AS L4 "
				+ " ON L4._objectTwo_ID = AB.ID "
				+ "LEFT/INNER JOIN ObjectOne AS ABB "
				+ " ON L4._objectOne_ID = ABB.ID"
				+ "LEFT/INNER JOIN ObjectOneSimpleTest_SimpleTest AS L5 "
				+ " ON L5._objectOne_ID = ABB.ID "
				+ "LEFT/INNER JOIN SimpleTest AS ABBA "
				+ " ON L5._simpleTest_ID = ABBA.ID";
				
				
				assertEquals(expected, actual);
						
				
	}
}