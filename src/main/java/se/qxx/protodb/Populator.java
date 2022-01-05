package se.qxx.protodb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Message.Builder;

public class Populator {
	
	public static void populateBasicFields(int id, Builder b, ProtoDBScanner scanner, ResultSet rs) throws SQLException {
		for (FieldDescriptor field : scanner.getBasicFields()) {
			Logger.log(String.format("Populating basic field :: %s", field.getName()));
			
			if (field.getName().equalsIgnoreCase("ID")) {
				b.setField(field, id);
			}
			else {
				Object o = rs.getObject(field.getName().toLowerCase());
				populateField(b, field, o);
			}
		}
	}

	public static void populateField(Builder b, FieldDescriptor field, Object o) {
		if (o == null)
			return;
		
		if (field.getJavaType() == JavaType.DOUBLE)
			if (o instanceof Double)
				b.setField(field, (double)o);
			else
				b.setField(field, (float)o);
		else if (field.getJavaType() == JavaType.FLOAT)
			if (o instanceof Double)
				b.setField(field, ((Double)o).floatValue());
			else
				b.setField(field, (float)o);
		else if (field.getJavaType() == JavaType.INT)
			if (o instanceof Long)
				b.setField(field, ((Long)o).intValue()); 
			else						
				b.setField(field, ((Integer)o).intValue());
		else if (field.getJavaType() == JavaType.LONG)
			if (o instanceof Long)
				b.setField(field, ((Long)o).longValue()); 
			else						
				b.setField(field, ((Integer)o).longValue());
		else if (field.getJavaType() == JavaType.BOOLEAN ) {
			if (o instanceof Integer) 
				b.setField(field, ((int)o) == 1 ? true : false);
			else if (o instanceof Boolean)
				b.setField(field, (boolean)o);
			else
				b.setField(field, ((String)o).equals("Y") ? true : false);	
		}
		else if (field.getJavaType() == JavaType.ENUM) {
			EnumValueDescriptor ev = field.getEnumType().findValueByName((String)o);
			b.setField(field, ev);
		}
		else if (field.getJavaType() == JavaType.BYTE_STRING) {
			if (o != null)
				b.setField(field, ByteString.copyFrom((byte[])o));
		}
			
		else
			b.setField(field, o);
		
		
		;
	}

	public static void populateObjectFields(ProtoDB db, Connection conn, Builder b, ProtoDBScanner scanner, ResultSet rs, List<String> excludedObjects)
			throws SQLException {
		
		for (FieldDescriptor field : scanner.getObjectFields()) {
			Logger.log(String.format("Populating object fields :: %s", field.getName()));
			
			int otherID = rs.getInt(scanner.getObjectFieldName(field));
			Logger.log(String.format("OtherID :: %s", otherID));
			
			if (field.getJavaType() == JavaType.MESSAGE && !isExcludedField(field.getName(), excludedObjects)) {
				DynamicMessage innerInstance = DynamicMessage.getDefaultInstance(field.getMessageType());
				DynamicMessage otherMsg = db.get(otherID, stripExcludedFields(field.getName(), excludedObjects), innerInstance, conn);
				
				if (otherMsg != null)
					b.setField(field, otherMsg);
			}
			else if (field.getJavaType() == JavaType.ENUM){
				//TODO: this is wrong. Should map correctly from database value instead
				//proto3 changes so that the first value always is 0 (and database ID starts at 1)
				int firstNumber = field.getEnumType().getValues().get(0).getNumber();				
				int adjustment = firstNumber == 1 ? 0 : -1;
				
				b.setField(field, field.getEnumType().findValueByNumber(otherID + adjustment));
			}
		}
	}
	
	private static byte[] getBlob(int otherID, Connection conn) throws SQLException {
		PreparedStatement prep = conn.prepareStatement("SELECT data FROM BlobData WHERE ID = ?");
		prep.setInt(1, otherID);
		byte[] data = null;
		
		ResultSet rs = prep.executeQuery();
		while(rs.next()){
			data = rs.getBytes("data");
		}
		rs.close();

		
		return data;
	}
	
//	protected static <T extends Message> List<T> getLinkObjectJoin(
//			ProtoDB db
//		, List<Integer> ids
//	    , List<String> excludedObjects
//		, T instance			
//		, FieldDescriptor field
//	    , Connection conn) throws SQLException {
//		
//		ProtoDBScanner scanner = new ProtoDBScanner((MessageOrBuilder)instance);
//		
//		Descriptor mt = field.getMessageType();
//		DynamicMessage mg = DynamicMessage.getDefaultInstance(mt);
//		
//		if (mg instanceof MessageOrBuilder) {
//			if (!isExcludedField(field.getName(), excludedObjects)) {
//			
//				MessageOrBuilder b2 = (MessageOrBuilder)mg;
//				ProtoDBScanner other = new ProtoDBScanner(b2);
//			
//				if (field.isRepeated()) {
//					// get select statement for link table
//					// we could join this into the next query. but small steps at a time
//					
//					String sql = scanner.getLinkTableSelectStatementIn(other, field.getName());
//					sql = String.format(sql, StringUtils.join(ids, ","));
//					Logger.log(sql);
//					
//					PreparedStatement prep = conn.prepareStatement(sql);
//					ResultSet rs = prep.executeQuery();
//					
//					// query returns ID's of the sub-object.
//					// create a list and use as a basis for the join query below
//					// but we need to tie this 
//					List<Integer> subIds = new ArrayList<Integer>();
//					while(rs.next()) {
//						subIds.add(rs.getInt(0));
//					}
//					
//					List<T> result = db.search(mg, "ID", StringUtils.join(ids, ","), ProtoDBSearchOperator.In);
//					
//					int c = 0;
//					while(rs.next()) {
//						// get sub objects
//						db.getByJoin(listOfObjects)
//						DynamicMessage otherMsg = db.get(rs.getInt("ID"), stripExcludedFields(field.getName(), excludedObjects), mg, conn);
//						b.addRepeatedField(field, otherMsg);
//						c++;
//					}
//					
//					Logger.log(String.format("Number of records retreived :: %s", c));
//					
//					rs.close();
//				}
//			}
//		}
//		
//	}
	
	protected static void getLinkObject(ProtoDB db
			, int id
			, List<String> excludedObjects
			, Builder b
			, ProtoDBScanner scanner			
			, FieldDescriptor field
			, Connection conn)
			throws SQLException {
		
		Descriptor mt = field.getMessageType();
		DynamicMessage mg = DynamicMessage.getDefaultInstance(mt);
		
		if (mg instanceof MessageOrBuilder) {
			if (!isExcludedField(field.getName(), excludedObjects)) {
			
				MessageOrBuilder b2 = (MessageOrBuilder)mg;
				ProtoDBScanner other = new ProtoDBScanner(b2, scanner.getBackend());
			
				if (field.isRepeated()) {
					// get select statement for link table
					String sql = scanner.getLinkTableSelectStatement(other, field.getName());
					Logger.log(sql);
					
					PreparedStatement prep = conn.prepareStatement(sql);
					prep.setInt(1, id);
					
					ResultSet rs = prep.executeQuery();
					
					int c = 0;
					while(rs.next()) {
						// get sub objects
						DynamicMessage otherMsg = db.get(rs.getInt("ID"), stripExcludedFields(field.getName(), excludedObjects), mg, conn);
						b.addRepeatedField(field, otherMsg);
						c++;
					}
					
					Logger.log(String.format("Number of records retreived :: %s", c));
					
					rs.close();
				}
			}
		}
	}

	public static void populateRepeatedObjectFields(
			ProtoDB db, 
			int id, 
			List<String> excludedObjects, 
			Connection conn, 
			Builder b, 
			ProtoDBScanner scanner)
			throws SQLException {
		
		for (FieldDescriptor field : scanner.getRepeatedObjectFields()) {
			Logger.log(String.format("Populating repeated object field :: %s", field.getName()));
			getLinkObject(db, id, excludedObjects, b, scanner, field, conn);
		}
	}


	public static void populateBlobs(Connection conn, Builder b, ProtoDBScanner scanner, ResultSet rs) throws SQLException {
		for (FieldDescriptor field : scanner.getBlobFields()) {
			int otherID = rs.getInt(scanner.getObjectFieldName(field));
			Logger.log(String.format("Populating blob id :: %s", otherID));
			
			byte[] data = getBlob(otherID, conn);
			
			if (data != null)
				b.setField(field, ByteString.copyFrom(data));
		}
	}

	public static void populateRepeatedBasicFields(int id, Connection conn, Builder b, ProtoDBScanner scanner)
			throws SQLException {
		for (FieldDescriptor field : scanner.getRepeatedBasicFields()) {
			Logger.log(String.format("Populating repeated basic field :: %s", field.getName()));
			
			String sql = scanner.getBasicLinkTableSelectStatement(field);
			Logger.log(sql);
			
			PreparedStatement prep = conn.prepareStatement(sql);
			prep.setInt(1, id);
			
			ResultSet rs = prep.executeQuery();
			
			while (rs.next()) {
				b.addRepeatedField(field, rs.getObject("value"));
			}
			rs.close();
		}
	}
	


	public static boolean isExcludedField(String name, List<String> excludedObjects) {
		if (excludedObjects == null)
			return false;
		
		for (String fields : excludedObjects) {
			String[] fieldParts = StringUtils.split(fields, ".");
			
			if (fieldParts.length == 1 && StringUtils.equalsIgnoreCase(name, fieldParts[0]))
				return true;
		}
		
		return false;
	}
	
	public static List<String> stripExcludedFields(String firstField, List<String> excludedObjects) {
		List<String> strippedList = new ArrayList<String>();
		
		if (excludedObjects != null) {
			for (String fields : excludedObjects) {
				int ii = fields.indexOf(".");
	
				if (ii >= 0 && StringUtils.equalsIgnoreCase(fields.substring(0, ii), firstField))
					strippedList.add(fields.substring(ii + 1));
			}
		}
		
		return strippedList;
	}

	public static void populateRequiredBlobs(Builder b, ProtoDBScanner scanner) {
		for(FieldDescriptor field : scanner.getBlobFields()) {
			if (field.isRequired())
				b.setField(field, ByteString.EMPTY);
		}
		
	}

}
