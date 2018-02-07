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
import se.qxx.protodb.test.TestDomain.EnumOne;
import se.qxx.protodb.test.TestDomain.Rating;
import se.qxx.protodb.test.TestDomain.RepObjectOne;
import se.qxx.protodb.test.TestDomain.SimpleTwo;

@RunWith(Parameterized.class)
public class TestSearchBlob {
	ProtoDB db = null;
	
	@Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(TestConstants.TEST_PARAMS);
    }
    
    public TestSearchBlob(String driver, String connectionString) throws DatabaseNotSupportedException {
    	db = ProtoDBFactory.getInstance(driver, connectionString);
    	
    	if (ProtoDBFactory.isSqlite(driver)) {
    		File f = new File(connectionString);
    			f.delete();
    	}

    }
    
	@Before
	public void Setup() throws ClassNotFoundException, SQLException, IDFieldNotFoundException {
		
		db.setupDatabase(TestDomain.SimpleTest.newBuilder());

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
		
		db.save(t);
	}
	
	
	@Test
	public void TestBlobSql() {
		TestDomain.SimpleTest o1 = TestDomain.SimpleTest.getDefaultInstance();
		
		ProtoDBScanner scanner = new ProtoDBScanner(o1, db.getDatabaseBackend());
		JoinResult result = Searcher.getJoinQuery(scanner, true, true);
		
		String expected = "SELECT "
				+ "A.[ID] AS A_ID, "
				+ "A.[dd] AS A_dd, "
				+ "A.[ff] AS A_ff, "
				+ "A.[is] AS A_is, "
				+ "A.[il] AS A_il, "
				+ "A.[bb] AS A_bb, "
				+ "A.[ss] AS A_ss, "
				+ "AA.[data] AS A_by "
				+ "FROM   SimpleTest AS A   "
				+ "LEFT JOIN BlobData AS AA "
				+ " ON A._by_ID = AA.ID ";
		
		assertEquals(expected, result.getJoinClause());
	}
	
	@Test
	public void TestBlobPopulator() throws ClassNotFoundException, SQLException, SearchFieldNotFoundException, ProtoDBParserException {
		TestDomain.SimpleTest o1 = TestDomain.SimpleTest.getDefaultInstance();

		List<TestDomain.SimpleTest> result = db.search(o1, "ss", "ThisIsATest", ProtoDBSearchOperator.Equals);
		
		assertEquals(1, result.size());
		assertEquals(3, result.get(0).getBy().size());
		assertEquals(5, result.get(0).getBy().byteAt(0));
		assertEquals(8, result.get(0).getBy().byteAt(1));
		assertEquals(6, result.get(0).getBy().byteAt(2));
		
	}
}
