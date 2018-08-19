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
import se.qxx.protodb.test.TestDomain.ObjectTwo;

@RunWith(Parameterized.class)
public class TestAddField extends TestBase {
	ProtoDB db = null;
	
	@Parameters
    public static Collection<Object[]> data() {
    	return getParams("selectParamsFile");
    }
    
    public TestAddField(String driver, String connectionString) throws DatabaseNotSupportedException {
    	db = ProtoDBFactory.getInstance(driver, connectionString);
    }	

	
	@Test
	public void TestAddBasicField() {	
		try {
			TestDomain.RepObjectOne b = db.get(1, TestDomain.RepObjectOne.getDefaultInstance());
			FieldDescriptor field = ObjectTwo.getDescriptor().findFieldByName("otis");
			
			db.addField(b, field);
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
