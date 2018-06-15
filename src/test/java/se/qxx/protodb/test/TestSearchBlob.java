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
public class TestSearchBlob extends TestBase{
	ProtoDB db = null;
	
	@Parameters
    public static Collection<Object[]> data() {
    	return getParams("testParamsFile");
    }
    
    public TestSearchBlob(String driver, String connectionString) throws DatabaseNotSupportedException, ClassNotFoundException, SQLException {
    	db = ProtoDBFactory.getInstance(driver, connectionString);
    	
    	clearDatabase(db, connectionString);
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
		
		String expected = 
			String.format(
				"SELECT "
				+ "A.%1$sID%2$s AS A_ID, "
				+ "A.%1$sdd%2$s AS A_dd, "
				+ "A.%1$sff%2$s AS A_ff, "
				+ "A.%1$sis%2$s AS A_is, "
				+ "A.%1$sil%2$s AS A_il, "
				+ "A.%1$sbb%2$s AS A_bb, "
				+ "A.%1$sss%2$s AS A_ss, "
				+ "AA.%1$sdata%2$s AS A_by "
				+ "FROM SimpleTest AS A "
				+ "LEFT JOIN BlobData AS AA "
				+ "ON A._by_ID = AA.ID ",
				db.getDatabaseBackend().getStartBracket(),
				db.getDatabaseBackend().getEndBracket());
		
		assertEquals(expected, result.getSql());
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
