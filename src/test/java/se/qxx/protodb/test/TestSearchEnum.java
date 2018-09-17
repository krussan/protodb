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
import se.qxx.protodb.test.TestDomain.EnumOne;
import se.qxx.protodb.test.TestDomain.Rating;
import se.qxx.protodb.test.TestDomain.RepObjectOne;
import se.qxx.protodb.test.TestDomain.SimpleTwo;

@RunWith(Parameterized.class)
public class TestSearchEnum extends TestBase {
	ProtoDB db = null;
	
	@Parameters
    public static Collection<Object[]> data() {
    	return getParams("testParamsFile");
    }
    
    public TestSearchEnum(String driver, String connectionString) throws DatabaseNotSupportedException, ClassNotFoundException, SQLException {
    	db = ProtoDBFactory.getInstance(driver, connectionString);
    	
    	clearDatabase(db, connectionString);
    }
    
	@Before
	public void Setup() throws ClassNotFoundException, SQLException, IDFieldNotFoundException {
		
	    db.setupDatabase(TestDomain.EnumOne.newBuilder());

		EnumOne o1 = EnumOne.newBuilder()
				.setID(1)
				.setEnumRating(Rating.ExactMatch)
				.setTitle("ExactMatch")
				.build();

		EnumOne o2 = EnumOne.newBuilder()
				.setID(2)
				.setEnumRating(Rating.PositiveMatch)
				.setTitle("PositiveMatch")
				.build();
		
		db.save(o1);
		db.save(o2);
	}
	
	@Test
	public void TestSearchEnumByJoin() {	
		try {
			List<TestDomain.EnumOne> result =
				db.search(
					SearchOptions.newBuilder(TestDomain.EnumOne.getDefaultInstance())
					.addFieldName("enumRating")
					.addSearchArgument("ExactMatch")
					.addOperator(ProtoDBSearchOperator.Equals));
			
			// we should get one single result..
			assertEquals(1, result.size());
			
			// we should get three sub results
			assertEquals(1, result.get(0).getID());

		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException  | ProtoDBParserException | SearchOptionsNotInitializedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void TestSearchEnumSql() {
		EnumOne o1 = EnumOne.getDefaultInstance();
		
		ProtoDBScanner scanner = new ProtoDBScanner(o1, db.getDatabaseBackend());
		JoinResult result = Searcher.getJoinQuery(scanner, false, true);
		
		String expected =
			String.format(
				"SELECT "
				+ "A.%1$sID%2$s AS A_ID, "
				+ "A.%1$stitle%2$s AS A_title, "
				+ "AA.%1$svalue%2$s AS A_enumRating "
				+ "FROM EnumOne AS A "
				+ "LEFT JOIN Rating AS AA "
				+ "ON A._enumRating_ID = AA.ID ",
				db.getDatabaseBackend().getStartBracket(),
				db.getDatabaseBackend().getEndBracket());
		
		assertEquals(expected, result.getSql());
	}
	
}
