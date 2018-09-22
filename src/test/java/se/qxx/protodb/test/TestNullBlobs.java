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

import com.google.protobuf.Descriptors.FieldDescriptor;

import se.qxx.protodb.ProtoDB;
import se.qxx.protodb.ProtoDBFactory;
import se.qxx.protodb.SearchOptions;
import se.qxx.protodb.exceptions.DatabaseNotSupportedException;
import se.qxx.protodb.exceptions.FieldNotFoundException;
import se.qxx.protodb.exceptions.IDFieldNotFoundException;
import se.qxx.protodb.exceptions.ProtoDBParserException;
import se.qxx.protodb.exceptions.SearchFieldNotFoundException;
import se.qxx.protodb.exceptions.SearchOptionsNotInitializedException;
import se.qxx.protodb.model.ProtoDBSearchOperator;
import se.qxx.protodb.test.TestDomain.ObjectFour;
import se.qxx.protodb.test.TestDomain.ObjectOne;
import se.qxx.protodb.test.TestDomain.ObjectTwo;
import se.qxx.protodb.test.TestDomain.SimpleTest;

@RunWith(Parameterized.class)
public class TestNullBlobs extends TestBase {
	ProtoDB db = null;

	@Parameters
    public static Collection<Object[]> data() {
    	return getParams("testParamsFile");
    }
    
    public TestNullBlobs(String driver, String connectionString) throws DatabaseNotSupportedException, ClassNotFoundException, SQLException {
    	db = ProtoDBFactory.getInstance(driver, connectionString);
    	
    	clearDatabase(db, connectionString);
    }	
    
    @SuppressWarnings("unchecked")
	@Test
    public void TestBlobsArePopulated() throws ClassNotFoundException, SQLException, SearchFieldNotFoundException, ProtoDBParserException, SearchOptionsNotInitializedException, IDFieldNotFoundException {
    	db.setupDatabase(SimpleTest.getDefaultInstance());
    	
    	SimpleTest o = SimpleTest.newBuilder().setID(-1).setSs("Null byte test")
    			.build();
    	
    	o = db.save(o);
    	
    	SearchOptions<SimpleTest> so = SearchOptions.newBuilder(SimpleTest.getDefaultInstance())
    			.addFieldName("ss")
    			.addOperator(ProtoDBSearchOperator.Equals)
    			.addSearchArgument("Null byte test");
    			
		List<SimpleTest> result = db.search(so);
		
		assertEquals(1, result.size());
		assertNotNull(result.get(0));
		assertEquals("Null byte test", result.get(0).getSs());
		assertNotNull(result.get(0).getBy());
		
			
    }

}
