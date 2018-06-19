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
import se.qxx.protodb.test.TestDomain.ObjectTwo;

@RunWith(Parameterized.class)
public class TestSearchRecursive extends TestBase {
	ProtoDB db = null;
	
	@Parameters
    public static Collection<Object[]> data() {
    	return getParams("testParamsFile");
    }
    
    public TestSearchRecursive(String driver, String connectionString) throws DatabaseNotSupportedException, ClassNotFoundException, SQLException {
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
		
		String expected =
			String.format(
				"SELECT "
				+ "A.%1$sID%2$s AS A_ID, "
				+ "AA.%1$sID%2$s AS AA_ID, "
				+ "AA.%1$sdd%2$s AS AA_dd, "
				
				+ "AA.%1$sff%2$s AS AA_ff, "
				+ "AA.%1$sis%2$s AS AA_is, "
				+ "AA.%1$sil%2$s AS AA_il, "
				+ "AA.%1$sbb%2$s AS AA_bb, "
				+ "AA.%1$sss%2$s AS AA_ss, "
				//+ "AA.by AS AA_by, "
				+ "AB.%1$sID%2$s AS AB_ID, "
				+ "AB.%1$sotis%2$s AS AB_otis, "
				+ "ABA.%1$sID%2$s AS ABA_ID, "
				+ "ABA.%1$sdd%2$s AS ABA_dd, "
				+ "ABA.%1$sff%2$s AS ABA_ff, "
				+ "ABA.%1$sis%2$s AS ABA_is, "
				+ "ABA.%1$sil%2$s AS ABA_il, "
				+ "ABA.%1$sbb%2$s AS ABA_bb, "
				+ "ABA.%1$sss%2$s AS ABA_ss, "
//				+ "ABA.by AS ABA_by, "
				+ "ABB.%1$sID%2$s AS ABB_ID, "
				+ "ABB.%1$soois%2$s AS ABB_oois, "
				+ "ABBA.%1$sID%2$s AS ABBA_ID, "
				+ "ABBA.%1$sdd%2$s AS ABBA_dd, "
				+ "ABBA.%1$sff%2$s AS ABBA_ff, "
				+ "ABBA.%1$sis%2$s AS ABBA_is, "
				+ "ABBA.%1$sil%2$s AS ABBA_il, "
				+ "ABBA.%1$sbb%2$s AS ABBA_bb, "
				+ "ABBA.%1$sss%2$s AS ABBA_ss "
//				+ "ABBA.by AS ABBA_by "
				+ "FROM ObjectThree AS A "
				+ "LEFT JOIN SimpleTest AS AA "
				+ "ON A._apa_ID = AA.ID "
				+ "LEFT JOIN ObjectTwo AS AB "
				+ "ON A._bepa_ID = AB.ID "
				+ "LEFT JOIN SimpleTest AS ABA "
				+ "ON AB._testone_ID = ABA.ID "
				+ "LEFT JOIN ObjectOne AS ABB "
				+ "ON AB._testtwo_ID = ABB.ID "
				+ "LEFT JOIN SimpleTest AS ABBA "
				+ "ON ABB._testone_ID = ABBA.ID ",
				db.getDatabaseBackend().getStartBracket(),
				db.getDatabaseBackend().getEndBracket());
				
				
		assertEquals(expected, result.getSql());

				
	}
	
	@Test
	public void TestSearchExactByJoin() {	
		try {
			List<TestDomain.ObjectThree> result =
				db.search(
						SearchOptions.newBuilder(TestDomain.ObjectThree.getDefaultInstance())
						.addSearchArgument("ThisIsATestOfObjectOne")
						.addFieldName("bepa.testTwo.testOne.ss")
						.addOperator(ProtoDBSearchOperator.Equals)
						.setShallow(true));						
					
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
		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException | ProtoDBParserException | SearchOptionsNotInitializedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
}
