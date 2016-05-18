package se.qxx.protodb.test;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.SQLException;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import se.qxx.protodb.ProtoDB;
import se.qxx.protodb.exceptions.IDFieldNotFoundException;

import com.google.protobuf.ByteString;

public class TestDelete {
	ProtoDB db = null;
	
	private final String DATABASE_FILE = "protodb_test.db";
	
	@Before
	public void Setup() {
		
		File f = new File(DATABASE_FILE);
		f.delete();
		
	    db = new ProtoDB(DATABASE_FILE);
	}
	
	
	@Test
	public void TestSimple() {		
		TestDomain.SimpleTest t = TestDomain.SimpleTest.newBuilder()
				.setID(-1)
				.setBb(false)
				.setBy(ByteString.copyFrom(new byte[] {5,8,6}))
				.setDd(1467802579378.62352352)
				.setFf((float) 555444333.213)
				.setIl(999999998)
				.setIs(999999998)
				.setSs("ThisIsATest")
				.build();
		
		try {
			db.setupDatabase(t);
			
			TestDomain.SimpleTest tt = db.save(t);

			// check to see if the save was successful (ID should be greater than 0)
			assertNotEquals(tt.getID(), -1);
			
			db.delete(tt);
			
			TestDomain.SimpleTest st = db.get(tt.getID(), TestDomain.SimpleTest.getDefaultInstance());
			
			assertNull(st);
			
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
			
			db.delete(tt);
			
			TestDomain.RepSimpleList st = db.get(tt.getID(), TestDomain.RepSimpleList.getDefaultInstance());

			assertNull(st);
			
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
						.setFf((float) 555444333.213)
						.setIl(999999998)
						.setIs(999999998)
						.setSs("ThisIsATestOfObjectOne")
				).build();
		
		TestDomain.SimpleTest tt = t.getTestOne();

		try {
			db.setupDatabase(t);
			
			TestDomain.ObjectOne ttt = db.save(t);
			
			assertNotEquals(ttt.getID(), -1);

			db.delete(ttt);
			
			TestDomain.ObjectOne oo = db.get(ttt.getID(), TestDomain.ObjectOne.getDefaultInstance());
			
			assertNull(oo);
			
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
	public void TestEnumOne() {
		TestDomain.EnumOne t = TestDomain.EnumOne.newBuilder()
			.setID(-1)
			.setRating(TestDomain.Rating.PositiveMatch)
			.setTitle("ThisIsAnEnumTitle")
			.build();
		
		try {
			db.setupDatabase(t);
			
			TestDomain.EnumOne oo = db.save(t);
			//int id = db.save(t);	
			
			assertNotEquals(oo.getID(), -1);
			assertEquals(t.getRating().toString(), oo.getRating().toString());
			assertEquals(t.getTitle(), oo.getTitle());

			db.delete(oo);
			
			TestDomain.EnumOne pp = db.get(oo.getID(), TestDomain.EnumOne.getDefaultInstance());
			
			assertNull(pp);
						
		} catch (SQLException | ClassNotFoundException | IDFieldNotFoundException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
	}	

}
