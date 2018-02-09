package se.qxx.protodb.test;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import se.qxx.protodb.ProtoDB;
import se.qxx.protodb.ProtoDBFactory;
import se.qxx.protodb.backend.ColumnDefinition;
import se.qxx.protodb.exceptions.DatabaseNotSupportedException;
import se.qxx.protodb.exceptions.IDFieldNotFoundException;
import se.qxx.protodb.test.TestDomain.Rating;

@RunWith(Parameterized.class)
public class TestSetup extends TestBase {
	ProtoDB db = null;
	
	@Parameters
    public static Collection<Object[]> data() {
    	return getParams("testParamsFile");
    }
    
    public TestSetup(String driver, String connectionString) throws DatabaseNotSupportedException, ClassNotFoundException, SQLException {
    	db = ProtoDBFactory.getInstance(driver, connectionString);
    	
    	if (ProtoDBFactory.isSqlite(driver)) {
    		File f = new File(connectionString);
    			f.delete();
    	} 
    	
    	
    	if (ProtoDBFactory.isMySql(driver)) {
    		db.dropAllTables();
    	}

    }	
	
	private final String[] SIMPLE_FIELD_NAMES = {"ID", "_by_ID", "dd", "ff", "is", "il", "bb", "ss"};
	private final String[] SIMPLE_FIELD_TYPES = {"INTEGER", "INTEGER", "DOUBLE", "FLOAT", "INTEGER", "BIGINT", "BIT", "LONGVARCHAR"};

	private final String[] OBJECTONE_FIELD_NAMES = {"ID", "_testone_ID", "oois"};
	private final String[] OBJECTONE_FIELD_TYPES = {"INTEGER", "INTEGER", "INTEGER"};

	private final String[] OBJECTTWO_FIELD_NAMES = {"ID", "_testone_ID", "_testtwo_ID", "otis"};
	private final String[] OBJECTTWO_FIELD_TYPES = {"INTEGER", "INTEGER", "INTEGER", "INTEGER"};

	private final String[] REPOBJECTONE_FIELD_NAMES = {"ID", "happycamper"};
	private final String[] REPOBJECTONE_FIELD_TYPES = {"INTEGER", "INTEGER"};

	private final String[] REPOBJECTONE_LINK_FIELD_NAMES = {"_repobjectone_ID", "_simpletwo_ID"};
	private final String[] REPOBJECTONE_LINK_FIELD_TYPES = {"INTEGER", "INTEGER"};

	private final String[] SIMPLETWO_FIELD_NAMES = {"ID", "title", "director"};
	private final String[] SIMPLETWO_FIELD_TYPES = {"INTEGER", "LONGVARCHAR", "LONGVARCHAR"};

	private final String[] REPSIMPLELIST_FIELD_NAMES = {"ID", "happycamper"};
	private final String[] REPSIMPLELIST_FIELD_TYPES = {"INTEGER", "INTEGER"};
	
	private final String[] REPSIMPLELIST_LISTOFSTRINGS_FIELD_NAMES = {"_repsimplelist_ID", "value"};
	private final String[] REPSIMPLELIST_LISTOFSTRINGS_FIELD_TYPES = {"INTEGER", "LONGVARCHAR"};

	private final String[] BLOBDATA_FIELD_NAMES = {"ID", "data"};
	private final String[] BLOBDATA_FIELD_TYPES = {"INTEGER", "LONGVARBINARY"};

	private final String[] ENUMONE_FIELD_NAMES = {"ID", "_rating_ID", "title"};
	private final String[] ENUMONE_FIELD_TYPES = {"INTEGER", "INTEGER", "LONGVARCHAR"};
	
	private final String[] RATING_FIELD_NAMES = {"ID", "value"};
	private final String[] RATING_FIELD_TYPES = {"INTEGER", "LONGVARCHAR"};

	private final String[] ENUMONELIST_FIELD_NAMES = {"ID", "title", "producer"};
	private final String[] ENUMONELIST_FIELD_TYPES = {"INTEGER", "LONGVARCHAR", "LONGVARCHAR"};
	
	private final String[] ENUMONELIST_LINK_FIELD_NAMES = {"_enumonelist_ID", "_rating_ID"};
	private final String[] ENUMONELIST_LINK_FIELD_TYPES = {"INTEGER", "INTEGER"};

	@Before
	public void Setup() {
		
		
	}
	

	@Test
	public void TestSimple() {
		
		TestDomain.SimpleTest t = TestDomain.SimpleTest.newBuilder().setID(1).build();
		
		try {
			db.setupDatabase(t);

			// test if database structure is the one we want
			testTableStructure(db, "SimpleTest", SIMPLE_FIELD_NAMES, SIMPLE_FIELD_TYPES);
			testTableStructure(db, "BlobData", BLOBDATA_FIELD_NAMES, BLOBDATA_FIELD_TYPES);

			
		} catch (SQLException | ClassNotFoundException | IDFieldNotFoundException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}


	@Test
	public void TestObjectOne() {
		TestDomain.ObjectOne t = TestDomain.ObjectOne.newBuilder().setID(1).build();

		try {
			db.setupDatabase(t);
			
			testTableStructure(db, "ObjectOne", OBJECTONE_FIELD_NAMES, OBJECTONE_FIELD_TYPES);
			testTableStructure(db, "SimpleTest", SIMPLE_FIELD_NAMES, SIMPLE_FIELD_TYPES);
			
		} catch (SQLException | ClassNotFoundException | IDFieldNotFoundException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}	
	
	@Test
	public void TestObjectTwo() {
		// test boolean
		TestDomain.ObjectTwo t = TestDomain.ObjectTwo.newBuilder().setID(1).build();

		try {
			db.setupDatabase(t);
			
			testTableStructure(db, "ObjectTwo", OBJECTTWO_FIELD_NAMES, OBJECTTWO_FIELD_TYPES);
			testTableStructure(db, "ObjectOne", OBJECTONE_FIELD_NAMES, OBJECTONE_FIELD_TYPES);
			testTableStructure(db, "SimpleTest", SIMPLE_FIELD_NAMES, SIMPLE_FIELD_TYPES);
			
		} catch (SQLException | ClassNotFoundException | IDFieldNotFoundException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		
		// test if database structure is the one we want
	}
	
	@Test
	public void TestRepObjectOne() {
		TestDomain.RepObjectOne t = TestDomain.RepObjectOne.newBuilder()
				.setID(1)
				.setHappycamper(42)
				.build();
		try {
			db.setupDatabase(t);
			
			testTableStructure(db, "SimpleTwo", SIMPLETWO_FIELD_NAMES, SIMPLETWO_FIELD_TYPES);
			testTableStructure(db, "RepObjectOne", REPOBJECTONE_FIELD_NAMES, REPOBJECTONE_FIELD_TYPES);			
			testTableStructure(db, "RepObjectOneSimpleTwo_ListOfObjects", REPOBJECTONE_LINK_FIELD_NAMES, REPOBJECTONE_LINK_FIELD_TYPES);
			
		} catch (SQLException | ClassNotFoundException | IDFieldNotFoundException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

	}
	
	@Test
	public void TestRepSimpleList() {
		TestDomain.RepSimpleList t = TestDomain.RepSimpleList.newBuilder()
				.setID(1)
				.setHappycamper(44)
				.build();
		
		try {
			db.setupDatabase(t);
			
			testTableStructure(db, "RepSimpleList", REPSIMPLELIST_FIELD_NAMES, REPSIMPLELIST_FIELD_TYPES);
			testTableStructure(db, "RepSimpleList_ListOfStrings", REPSIMPLELIST_LISTOFSTRINGS_FIELD_NAMES, REPSIMPLELIST_LISTOFSTRINGS_FIELD_TYPES);			
			
		} catch (SQLException | ClassNotFoundException | IDFieldNotFoundException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		
	}

	@Test
	public void TestEnumOne() {
		TestDomain.EnumOne t = TestDomain.EnumOne.newBuilder()
				.setID(-1)
				.setTitle("ThisIsAnEnumTitle")
				.setRating(Rating.SubsExist)
				.build();
		
		try {
			db.setupDatabase(t);
			
			testTableStructure(db, "EnumOne", ENUMONE_FIELD_NAMES, ENUMONE_FIELD_TYPES);
			testTableStructure(db, "Rating", RATING_FIELD_NAMES, RATING_FIELD_TYPES);			
			
		} catch (SQLException | ClassNotFoundException | IDFieldNotFoundException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

	}
	
	@Test
	public void TestEnumOneList() {
		TestDomain.EnumOneList t = TestDomain.EnumOneList.newBuilder()
				.setID(-1)
				.setTitle("ThisIsAnEnumListTitle")
				.build();
		
		try {
			db.setupDatabase(t);
			
			testTableStructure(db, "EnumOneList", ENUMONELIST_FIELD_NAMES, ENUMONELIST_FIELD_TYPES);
			testTableStructure(db, "Rating", RATING_FIELD_NAMES, RATING_FIELD_TYPES);			
			testTableStructure(db, "EnumOneListRating_ListOfRatings", ENUMONELIST_LINK_FIELD_NAMES, ENUMONELIST_LINK_FIELD_TYPES);
			
		} catch (SQLException | ClassNotFoundException | IDFieldNotFoundException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

	}
		
	
	
	private void testTableStructure(
			ProtoDB db,
			String expectedTableName,
			String[] fieldNames, 
			String[] fieldTypes) throws ClassNotFoundException {
		try {
			List<ColumnDefinition> cols = db.retreiveColumns(expectedTableName);
			
			int c = 0;		
			for(ColumnDefinition col : cols) {
				JDBCType type = JDBCType.valueOf(fieldTypes[c]);
				String mappedType = db.getDatabaseBackend().getTypeMap(type);
				
				assertEquals(fieldNames[c], col.getName());
				assertEquals(mappedType, col.getType());
				c++;
			}
		}
		catch (SQLException e) {
			fail(String.format("Table %s failed the test. Probably because it does not exist. Msg :: %s", expectedTableName, e.getMessage()));
		}
	}	
	
	
}
