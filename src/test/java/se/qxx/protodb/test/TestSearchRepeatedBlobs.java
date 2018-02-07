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
import se.qxx.protodb.Searcher;
import se.qxx.protodb.exceptions.IDFieldNotFoundException;
import se.qxx.protodb.exceptions.ProtoDBParserException;
import se.qxx.protodb.exceptions.SearchFieldNotFoundException;
import se.qxx.protodb.model.ProtoDBSearchOperator;
import se.qxx.protodb.test.TestDomain.ObjectFour;
import se.qxx.protodb.test.TestDomain.RepObjectOne;
import se.qxx.protodb.test.TestDomain.SimpleTwo;

public class TestSearchRepeatedBlobs {
	ProtoDB db = null;
	
	private final String DATABASE_FILE = "protodb_repeated_blobs_test.db";

	@Before
	public void Setup() throws ClassNotFoundException, SQLException, IDFieldNotFoundException {
		
		File f = new File(DATABASE_FILE);
		if (f.exists())
			f.delete();
		
	    db = new ProtoDB(DATABASE_FILE);
	    
	    db.setupDatabase(TestDomain.ObjectFour.newBuilder());
		
	    ObjectFour o1 = ObjectFour.newBuilder()
	    		.setID(20)
	    		.setFourTitle("this is a repeated blob filler")
	    		.addFourImage(ByteString.copyFrom(new byte[] {5,8,6}))
	    		.addFourImage(ByteString.copyFrom(new byte[] {1,2,3}))
	    		.build();
	    
	    ObjectFour o2 = ObjectFour.newBuilder()
	    		.setID(20)
	    		.setFourTitle("ObjectFourTitle")
	    		.addFourImage(ByteString.copyFrom(new byte[] {9,8,7}))
	    		.addFourImage(ByteString.copyFrom(new byte[] {6,5,4}))
	    		.build();

	    db.save(o1);
		db.save(o2);
	}
	
	@Test
	public void TestSearchExactByJoin() {	
		try {
			List<TestDomain.ObjectFour> result =
				db.search(
					TestDomain.ObjectFour.getDefaultInstance(), 
					"fourtitle", 
					"ObjectFourTitle", 
					ProtoDBSearchOperator.Equals);
			
			
			// we should get one single result..
			assertEquals(1, result.size());
			
			// we should get three sub results
			assertEquals(2, result.get(0).getFourImageList().size());
			

		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException | ProtoDBParserException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
