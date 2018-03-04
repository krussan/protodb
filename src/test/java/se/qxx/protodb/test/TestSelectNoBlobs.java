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

import se.qxx.protodb.ProtoDB;
import se.qxx.protodb.ProtoDBFactory;
import se.qxx.protodb.exceptions.DatabaseNotSupportedException;
import se.qxx.protodb.exceptions.IDFieldNotFoundException;
import se.qxx.protodb.exceptions.SearchFieldNotFoundException;
import se.qxx.protodb.test.TestDomain.ObjectFive;
import se.qxx.protodb.test.TestDomain.ObjectFour;

@RunWith(Parameterized.class)
public class TestSelectNoBlobs extends TestBase {
	ProtoDB db = null;
	
	@Parameters
    public static Collection<Object[]> data() {
    	return getParams("testParamsFile");
    }
    
    public TestSelectNoBlobs(String driver, String connectionString) throws DatabaseNotSupportedException, ClassNotFoundException, SQLException {
    	db = ProtoDBFactory.getInstance(driver, connectionString);
    	
    	clearDatabase(db, connectionString);
    }	
    
    @Before
	public void Setup() throws ClassNotFoundException, SQLException, IDFieldNotFoundException {
	    db.setupDatabase(TestDomain.ObjectFive.newBuilder());
		
	    ObjectFive o1 = ObjectFive.newBuilder()
	    		.setID(20)
	    		.setFourTitle("this is a repeated blob filler")
	    		.setFourImage(ByteString.copyFrom(new byte[] {5,8,6}))
	    		.build();
	    
	    db.save(o1);
	}
	
	@Test
	public void TestRequiredBlobs() {	
		try {
			db.setPopulateBlobs(false);
			List<ObjectFive> list = db.find(ObjectFive.getDefaultInstance(), "fourTitle", "%", true);

			
			assertNotNull(list);
			assertEquals(1, list.size());
			
			// happyCamper should be 3
			assertEquals("this is a repeated blob filler", list.get(0).getFourTitle());
			assertEquals(ByteString.EMPTY, list.get(0).getFourImage());
			
		} catch (SQLException | ClassNotFoundException | SearchFieldNotFoundException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
