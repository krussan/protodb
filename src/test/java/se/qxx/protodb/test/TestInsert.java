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

import se.qxx.protodb.ProtoDB;
import se.qxx.protodb.ProtoDBFactory;
import se.qxx.protodb.exceptions.DatabaseNotSupportedException;
import se.qxx.protodb.exceptions.IDFieldNotFoundException;

import com.google.protobuf.ByteString;

@RunWith(Parameterized.class)
public class TestInsert extends TestBase {
	ProtoDB db = null;
	
	@Parameters
    public static Collection<Object[]> data() {
    	return getParams("testParamsFile");
    }
    
    public TestInsert(String driver, String connectionString) throws DatabaseNotSupportedException, ClassNotFoundException, SQLException {
    	db = ProtoDBFactory.getInstance(driver, connectionString);
    	
    	clearDatabase(db, connectionString);
	}
	
	@Test
	public void TestSimple() {		
		TestDomain.SimpleTest t = TestDomain.SimpleTest.newBuilder()
				.setID(-1)
				.setBb(false)
				.setBy(ByteString.copyFrom(new byte[] {5,8,6}))
				.setDd(1467802579378.62352352)
				.setFf((float) 5554.213)
				.setIl(999999998)
				.setIs(999999998)
				.setSs("ThisIsATest")
				.build();
		
		try {
			db.setupDatabase(t);
			
			TestDomain.SimpleTest tt = db.save(t);

			// check to see if the save was successful (ID should be greater than 0)
			assertNotEquals(tt.getID(), -1);
			
			
			TestDomain.SimpleTest st = db.get(tt.getID(), TestDomain.SimpleTest.getDefaultInstance());
			
			assertEquals(t.getBb(), st.getBb());
			assertEquals(t.getDd(), st.getDd(), 0.005);
			assertEquals(t.getFf(), st.getFf(), 0.005);
			assertEquals(t.getIl(), st.getIl());
			assertEquals(t.getIs(), st.getIs());
			assertNotEquals(t.getSs(), st.getSs());
			assertArrayEquals(t.getBy().toByteArray(), st.getBy().toByteArray());
			
//			PreparedStatement prep = "SELECT * FROM SimpleTest";
//			
//			testTableStructure(db, "SimpleTest", SIMPLE_FIELD_NAMES, SIMPLE_FIELD_TYPES);

			
		} catch (SQLException | ClassNotFoundException | IDFieldNotFoundException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void TestRepSimpleList() {		
		TestDomain.RepSimpleList t = TestDomain.RepSimpleList.newBuilder()
				.setID(-1)
				.setHappycamper(789)
				.addAllListOfStrings(Arrays.asList(new String[] {"simple", "types", "are", "fun"}))
				.build();
		
		try {
			db.setupDatabase(t);
			
			TestDomain.RepSimpleList tt = db.save(t);

			// check to see if the save was successful (ID should be greater than 0)
			assertNotEquals(tt.getID(), -1);
			
			TestDomain.RepSimpleList st = db.get(tt.getID(), TestDomain.RepSimpleList.getDefaultInstance());
			
			assertEquals(t.getHappycamper(), st.getHappycamper());
			assertArrayEquals(
				t.getListOfStringsList().toArray(new String[] {}), 
				st.getListOfStringsList().toArray(new String[] {}));
			
//			PreparedStatement prep = "SELECT * FROM SimpleTest";
//			
//			testTableStructure(db, "SimpleTest", SIMPLE_FIELD_NAMES, SIMPLE_FIELD_TYPES);

			
		} catch (SQLException | ClassNotFoundException | IDFieldNotFoundException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
		

	@Test
	public void TestObjectOne() {
		TestDomain.ObjectOne t = TestDomain.ObjectOne.newBuilder()
				.setID(-1)
				.setOois(986)
				.setTestOne(TestDomain.SimpleTest.newBuilder()
						.setID(-1)
						.setBb(false)
						.setBy(ByteString.copyFrom(new byte[] {5,8,6}))
						.setDd(1467802579378.62352352)
						.setFf((float) 5554.213)
						.setIl(999999998)
						.setIs(999999998)
						.setSs("ThisIsATestOfObjectOne")
				).build();
		
		TestDomain.SimpleTest tt = t.getTestOne();

		try {
			db.setupDatabase(t);
			
			TestDomain.ObjectOne ttt = db.save(t);
			
			assertNotEquals(ttt.getID(), -1);
			
			TestDomain.ObjectOne oo = db.get(ttt.getID(), TestDomain.ObjectOne.getDefaultInstance());
			TestDomain.SimpleTest st = oo.getTestOne();
			
			assertEquals(t.getOois(), oo.getOois());
			
			assertEquals(tt.getBb(), st.getBb());
			assertEquals(tt.getDd(), st.getDd(), 0.005);
			assertEquals(tt.getFf(), st.getFf(), 0.005);
			assertEquals(tt.getIl(), st.getIl());
			assertEquals(tt.getIs(), st.getIs());
			assertEquals(tt.getSs(), st.getSs());
			assertArrayEquals(tt.getBy().toByteArray(), st.getBy().toByteArray());

			
		} catch (SQLException | ClassNotFoundException | IDFieldNotFoundException  e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}	
	
	private void assertNotEquals(int a, int b) {
		String msg = String.format("Expected not to be actual. %s == %s", a, b);
		assertFalse(msg, a == b);
	}

	private void assertNotEquals(String a, String b) {
		String msg = String.format("Expected not to be actual. %s == %s", a, b);
		assertFalse(msg, a == b);
	}

	@Test
	public void TestSimpleUpdate() {		
		TestDomain.SimpleTest t = TestDomain.SimpleTest.newBuilder()
				.setID(-1)
				.setBb(false)
				.setBy(ByteString.copyFrom(new byte[] {5,8,6}))
				.setDd(1467802579378.62352352)
				.setFf((float) 5554.213)
				.setIl(999999998)
				.setIs(999999998)
				.setSs("ThisIsATest")
				.build();
		
		try {
			db.setupDatabase(t);
			
			TestDomain.SimpleTest tt = db.save(t);
			// check to see if the save was successful (ID should be greater than 0)
			assertNotEquals(tt.getID(), -1);
			
			tt = TestDomain.SimpleTest.newBuilder(tt)
				.setSs("ThisIsTheUpdateTest")
				.build();
			
			TestDomain.SimpleTest t2 = db.save(tt);
			assertNotEquals(t2.getID(), -1);			
			assertEquals(tt.getID(), t2.getID());
		
			TestDomain.SimpleTest st = db.get(t2.getID(), TestDomain.SimpleTest.getDefaultInstance());
			
			
			assertEquals(t.getBb(), st.getBb());
			assertEquals(t.getDd(), st.getDd(), 0.005);
			assertEquals(t.getFf(), st.getFf(), 0.005);
			assertEquals(t.getIl(), st.getIl());
			assertEquals(t.getIs(), st.getIs());
			assertNotEquals(t.getSs(), st.getSs());
			assertArrayEquals(t.getBy().toByteArray(), st.getBy().toByteArray());
			
//			PreparedStatement prep = "SELECT * FROM SimpleTest";
//			
//			testTableStructure(db, "SimpleTest", SIMPLE_FIELD_NAMES, SIMPLE_FIELD_TYPES);

			
		} catch (SQLException | ClassNotFoundException | IDFieldNotFoundException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void TestEnumOne() {
		TestDomain.EnumOne t = TestDomain.EnumOne.newBuilder()
			.setID(-1)
			.setEnumRating(TestDomain.Rating.PositiveMatch)
			.setTitle("ThisIsAnEnumTitle")
			.build();
		
		try {
			db.setupDatabase(t);
			
			TestDomain.EnumOne oo = db.save(t);
			//int id = db.save(t);	
			
			assertNotEquals(oo.getID(), -1);
			assertEquals(t.getEnumRating().toString(), oo.getEnumRating().toString());
			assertEquals(t.getTitle(), oo.getTitle());
			
			TestDomain.EnumOne pp = db.get(oo.getID(), TestDomain.EnumOne.getDefaultInstance());
			
			assertEquals(oo.getID(), pp.getID());
			assertEquals(oo.getEnumRating().toString(), pp.getEnumRating().toString());
			assertEquals(oo.getTitle(), pp.getTitle());
						
		} catch (SQLException | ClassNotFoundException | IDFieldNotFoundException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
	}
	
	@Test
	public void TestRepObjectOne() {
		TestDomain.SimpleTwo o1 = TestDomain.SimpleTwo.newBuilder()
				.setID(-1)
				.setDirector("Humpty dumpty")
				.setTitle("Sat on a wall")
				.build();
		
		TestDomain.SimpleTwo o2 = TestDomain.SimpleTwo.newBuilder()
				.setID(-1)
				.setDirector("Humle o dumle")
				.setTitle("Satt i ett skafferi")
				.build();

		TestDomain.SimpleTwo o3 = TestDomain.SimpleTwo.newBuilder()
				.setID(-1)
				.setDirector("Abrakadabra")
				.setTitle("Simsalabim")
				.build();

		TestDomain.RepObjectOne t = TestDomain.RepObjectOne.newBuilder()
				.setID(-1)
				.setHappycamper(42)
				.addListOfObjects(o1)
				.addListOfObjects(o2)
				.addListOfObjects(o3)
				.build();

		try {
			db.setupDatabase(t);
			
			TestDomain.RepObjectOne oo = db.save(t);
			//int id = db.save(t);	
			
			assertNotEquals(oo.getID(), -1);
			assertEquals(t.getHappycamper(), oo.getHappycamper());
			
			TestDomain.RepObjectOne pp = db.get(oo.getID(), TestDomain.RepObjectOne.getDefaultInstance());
			
			assertEquals(oo.getID(), pp.getID());
			assertEquals(3, pp.getListOfObjectsCount());
			
						
		} catch (SQLException | ClassNotFoundException | IDFieldNotFoundException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		

	}

}
