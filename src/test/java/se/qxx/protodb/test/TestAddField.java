package se.qxx.protodb.test;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.protobuf.Descriptors.FieldDescriptor;

import se.qxx.protodb.ProtoDB;
import se.qxx.protodb.ProtoDBFactory;
import se.qxx.protodb.exceptions.DatabaseNotSupportedException;
import se.qxx.protodb.exceptions.FieldNotFoundException;
import se.qxx.protodb.exceptions.IDFieldNotFoundException;
import se.qxx.protodb.test.TestDomain.ObjectOne;
import se.qxx.protodb.test.TestDomain.ObjectTwo;

@RunWith(Parameterized.class)
public class TestAddField extends TestBase {
	ProtoDB db = null;

	private final String[] REPOBJECTONE_FIELD_NAMES_ADD_BASIC = {"ID", "happycamper", "otis"};
	private final String[] REPOBJECTONE_FIELD_TYPES_ADD_BASIC = {"INTEGER", "INTEGER", "INTEGER"};

	private final String[] REPOBJECTONE_FIELD_NAMES_ADD_OBJECT = {"ID", "happycamper", "_testone_ID"};
	private final String[] REPOBJECTONE_FIELD_TYPES_ADD_OBJECT = {"INTEGER", "INTEGER", "INTEGER"};

	@Parameters
    public static Collection<Object[]> data() {
    	return getParams("selectParamsFile");
    }
    
    public TestAddField(String driver, String connectionString) throws DatabaseNotSupportedException, ClassNotFoundException, SQLException {
    	db = ProtoDBFactory.getInstance(driver, connectionString);
    	
    	clearDatabase(db, connectionString);

    }	

	
	@Test
	public void TestAddBasicField() {	
		try {
			TestDomain.RepObjectOne b = TestDomain.RepObjectOne.getDefaultInstance();
			FieldDescriptor field = ObjectTwo.getDescriptor().findFieldByName("otis");
			
			db.addField(b, field);
			
			TestSetup.testTableStructure(db, "RepObjectOne", REPOBJECTONE_FIELD_NAMES_ADD_BASIC, REPOBJECTONE_FIELD_TYPES_ADD_BASIC);
			
		} catch (SQLException | ClassNotFoundException | IDFieldNotFoundException | FieldNotFoundException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

//	@Test
//	public void TestAddObjectField() {	
//		try {
//			TestDomain.RepObjectOne b = TestDomain.RepObjectOne.getDefaultInstance();
//			FieldDescriptor field = ObjectOne.getDescriptor().findFieldByName("testOne");
//			
//			// this does not work in test as the new field is not a child of the current message.
//			// However in a real scenario this is not the case. A proto file has been modified so the field will be there
//			db.addField(b, field);
//			
//			TestSetup.testTableStructure(db, "RepObjectOne", REPOBJECTONE_FIELD_NAMES_ADD_OBJECT, REPOBJECTONE_FIELD_TYPES_ADD_OBJECT);
//			
//		} catch (SQLException | ClassNotFoundException | IDFieldNotFoundException | FieldNotFoundException e) {
//			e.printStackTrace();
//			fail(e.getMessage());
//		}
//	}
}
