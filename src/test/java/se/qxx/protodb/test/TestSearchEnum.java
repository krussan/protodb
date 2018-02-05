package se.qxx.protodb.test;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.SQLException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import se.qxx.protodb.JoinResult;
import se.qxx.protodb.ProtoDB;
import se.qxx.protodb.ProtoDBScanner;
import se.qxx.protodb.Searcher;
import se.qxx.protodb.exceptions.IDFieldNotFoundException;
import se.qxx.protodb.exceptions.ProtoDBParserException;
import se.qxx.protodb.exceptions.SearchFieldNotFoundException;
import se.qxx.protodb.model.ProtoDBSearchOperator;
import se.qxx.protodb.test.TestDomain.EnumOne;
import se.qxx.protodb.test.TestDomain.Rating;
import se.qxx.protodb.test.TestDomain.RepObjectOne;
import se.qxx.protodb.test.TestDomain.SimpleTwo;

public class TestSearchEnum {
	ProtoDB db = null;
	
	private final String DATABASE_FILE = "protodb_enum_test.db";

	@Before
	public void Setup() throws ClassNotFoundException, SQLException, IDFieldNotFoundException {
		
		File f = new File(DATABASE_FILE);
		if (f.exists())
			f.delete();
		
	    db = new ProtoDB(DATABASE_FILE);
	    
	    db.setupDatabase(TestDomain.EnumOne.newBuilder());

		EnumOne o1 = EnumOne.newBuilder()
				.setID(1)
				.setRating(Rating.ExactMatch)
				.setTitle("ExactMatch")
				.build();

		EnumOne o2 = EnumOne.newBuilder()
				.setID(2)
				.setRating(Rating.PositiveMatch)
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
					TestDomain.EnumOne.getDefaultInstance(), 
					"rating", 
					"ExactMatch", 
					ProtoDBSearchOperator.Equals);
			
			// we should get one single result..
			assertEquals(1, result.size());
			
			// we should get three sub results
			assertEquals(1, result.get(0).getID());

		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException  | ProtoDBParserException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void TestSearchEnumSql() {
		EnumOne o1 = EnumOne.getDefaultInstance();
		
		ProtoDBScanner scanner = new ProtoDBScanner(o1);
		JoinResult result = Searcher.getJoinQuery(scanner, false, true);
		
		String expected = "SELECT "
				+ "A.[ID] AS A_ID, "
				+ "A.[title] AS A_title, "
				+ "AA.[value] AS A_rating "
				+ "FROM   EnumOne AS A   "
				+ "LEFT JOIN Rating AS AA "
				+ " ON A._rating_ID = AA.ID ";
		
		assertEquals(expected, result.getJoinClause());
	}
	
}
