package se.qxx.protodb.test;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.SQLException;
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
import se.qxx.protodb.test.TestDomain.ObjectFour;
import se.qxx.protodb.test.TestDomain.RepObjectOne;
import se.qxx.protodb.test.TestDomain.SimpleTwo;

@RunWith(Parameterized.class)
public class TestSearchRepeatedBlobs extends TestBase {
	ProtoDB db = null;
	
	@Parameters
    public static Collection<Object[]> data() {
    	return getParams("testParamsFile");
    }
    
    public TestSearchRepeatedBlobs(String driver, String connectionString) throws DatabaseNotSupportedException, ClassNotFoundException, SQLException {
    	db = ProtoDBFactory.getInstance(driver, connectionString);
    	
    	clearDatabase(db, connectionString);
	}	
    
	@Before
	public void Setup() throws ClassNotFoundException, SQLException, IDFieldNotFoundException {
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
