package se.qxx.protodb.test;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.SQLException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.ByteString;

import se.qxx.protodb.JoinResult;
import se.qxx.protodb.ProtoDB;
import se.qxx.protodb.ProtoDBScanner;
import se.qxx.protodb.exceptions.IDFieldNotFoundException;
import se.qxx.protodb.exceptions.SearchFieldNotFoundException;
import se.qxx.protodb.test.TestDomain.ObjectTwo;
import se.qxx.protodb.test.TestDomain.RepObjectOne;
import se.qxx.protodb.test.TestDomain.SimpleTwo;

public class TestSearchRepeated {
	ProtoDB db = null;
	
	private final String DATABASE_FILE = "protodb_repeated_test.db";

	@Before
	public void Setup() throws ClassNotFoundException, SQLException, IDFieldNotFoundException {
		
		File f = new File(DATABASE_FILE);
		if (f.exists())
			f.delete();
		
	    db = new ProtoDB(DATABASE_FILE);
	    
	    db.setupDatabase(TestDomain.ObjectThree.newBuilder());
		
		RepObjectOne o1 = RepObjectOne.newBuilder()
				.setHappycamper(555)
				.addListOfObjects(SimpleTwo.newBuilder()
						.setDirector("directThis")
						.setTitle("thisisatitle")
						.build())
				.addListOfObjects(SimpleTwo.newBuilder()
						.setDirector("no_way")
						.setTitle("who_said_that")
						.build())
				.build();

		RepObjectOne o2 = RepObjectOne.newBuilder()
				.setHappycamper(444)
				.addListOfObjects(SimpleTwo.newBuilder()
						.setDirector("direction")
						.setTitle("up_side")
						.build())
				.build();

		db.save(o1);
		db.save(o2);
	}
	
	@Test
	public void TestSearchExactByJoin() {	
		try {
			List<TestDomain.RepObjectOne> result =
				db.search(
					TestDomain.RepObjectOne.getDefaultInstance(), 
					"bepa.testTwo.testOne.ss", 
					"ThisIsATestOfObjectOne", 
					false);
			
			// we should get one single result..
			assertEquals(1, result.size());

		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException  e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

}
