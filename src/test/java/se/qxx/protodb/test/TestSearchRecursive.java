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

import com.google.protobuf.ByteString;

import se.qxx.protodb.JoinResult;
import se.qxx.protodb.ProtoDB;
import se.qxx.protodb.ProtoDBFactory;
import se.qxx.protodb.ProtoDBScanner;
import se.qxx.protodb.Searcher;
import se.qxx.protodb.exceptions.DatabaseNotSupportedException;
import se.qxx.protodb.exceptions.IDFieldNotFoundException;
import se.qxx.protodb.exceptions.ProtoDBParserException;
import se.qxx.protodb.exceptions.SearchFieldNotFoundException;
import se.qxx.protodb.model.ProtoDBSearchOperator;
import se.qxx.protodb.test.TestDomain.ObjectTwo;

@RunWith(Parameterized.class)
public class TestSearchRecursive {
	ProtoDB db = null;
	
	@Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(TestConstants.TEST_PARAMS);
    }
    
    public TestSearchRecursive(String driver, String connectionString) throws DatabaseNotSupportedException {
    	db = ProtoDBFactory.getInstance(driver, connectionString);
    	
    	if (ProtoDBFactory.isSqlite(driver)) {
    		File f = new File(connectionString);
    			f.delete();
    	}

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
	    		
		ProtoDBScanner scanner = new ProtoDBScanner(o3, db.getDatabaseBackend());
		JoinResult result = Searcher.getJoinQuery(scanner, false, true);
		
		String expected = "SELECT "
				+ "A.[ID] AS A_ID, "
				+ "AA.[ID] AS AA_ID, "
				+ "AA.[dd] AS AA_dd, "
				
				+ "AA.[ff] AS AA_ff, "
				+ "AA.[is] AS AA_is, "
				+ "AA.[il] AS AA_il, "
				+ "AA.[bb] AS AA_bb, "
				+ "AA.[ss] AS AA_ss, "
				//+ "AA.by AS AA_by, "
				+ "AB.[ID] AS AB_ID, "
				+ "AB.[otis] AS AB_otis, "
				+ "ABA.[ID] AS ABA_ID, "
				+ "ABA.[dd] AS ABA_dd, "
				+ "ABA.[ff] AS ABA_ff, "
				+ "ABA.[is] AS ABA_is, "
				+ "ABA.[il] AS ABA_il, "
				+ "ABA.[bb] AS ABA_bb, "
				+ "ABA.[ss] AS ABA_ss, "
//				+ "ABA.by AS ABA_by, "
				+ "ABB.[ID] AS ABB_ID, "
				+ "ABB.[oois] AS ABB_oois, "
				+ "ABBA.[ID] AS ABBA_ID, "
				+ "ABBA.[dd] AS ABBA_dd, "
				+ "ABBA.[ff] AS ABBA_ff, "
				+ "ABBA.[is] AS ABBA_is, "
				+ "ABBA.[il] AS ABBA_il, "
				+ "ABBA.[bb] AS ABBA_bb, "
				+ "ABBA.[ss] AS ABBA_ss "
//				+ "ABBA.by AS ABBA_by "
				+ "FROM   ObjectThree AS A   "
				+ "LEFT JOIN SimpleTest AS AA "
				+ " ON A._apa_ID = AA.ID "
				+ "LEFT JOIN ObjectTwo AS AB "
				+ " ON A._bepa_ID = AB.ID "
				+ "LEFT JOIN SimpleTest AS ABA "
				+ " ON AB._testone_ID = ABA.ID "
				+ "LEFT JOIN ObjectOne AS ABB "
				+ " ON AB._testtwo_ID = ABB.ID "
				+ "LEFT JOIN SimpleTest AS ABBA "
				+ " ON ABB._testone_ID = ABBA.ID ";
				
				
				assertEquals(expected, result.getJoinClause());
						
				
	}
	
	@Test
	public void TestSearchExactByJoin() {	
		try {
			List<TestDomain.ObjectThree> result =
				db.search(
					TestDomain.ObjectThree.getDefaultInstance(), 
					"bepa.testTwo.testOne.ss", 
					"ThisIsATestOfObjectOne", 
					ProtoDBSearchOperator.Equals);
			
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
		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException | ProtoDBParserException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
}
