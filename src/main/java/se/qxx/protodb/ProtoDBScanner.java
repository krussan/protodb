package se.qxx.protodb;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.MessageOrBuilder;

import se.qxx.protodb.backend.DatabaseBackend;
import se.qxx.protodb.model.ProtoTable;

import com.google.protobuf.Descriptors.FieldDescriptor;

public class ProtoDBScanner {

	String objectName;
	private MessageOrBuilder message = null;
	private DatabaseBackend backend = null;
	
	private List<FieldDescriptor> objectFields = new ArrayList<FieldDescriptor>();
	private List<FieldDescriptor> repeatedObjectFields = new ArrayList<FieldDescriptor>();
	
	private List<FieldDescriptor> basicFields = new ArrayList<FieldDescriptor>();
	private List<FieldDescriptor> repeatedBasicFields = new ArrayList<FieldDescriptor>();
	
	private List<FieldDescriptor> blobFields  = new ArrayList<FieldDescriptor>();
	
	FieldDescriptor idField = null;
	

	private HashMap<String, Integer> objectIDs = new HashMap<String,Integer>();
	private HashMap<String, Integer> blobIDs = new HashMap<String,Integer>();

	private boolean isProto2 = false;
	
	public enum FieldType {
		ID,
		RepeatedObject,
		RepeatedEnum,
		RepeatedBasic,
		Object,
		Enum,
		Blob,
		Basic		
	}

	public ProtoDBScanner(MessageOrBuilder b, DatabaseBackend backend) {
		this.setMessage(b);
		this.setBackend(backend);
		this.scan(b);
	}
	
	private void scan(MessageOrBuilder b) {
		this.setObjectName(StringUtils.capitalize(b.getDescriptorForType().getName()));
		this.setProto2(this.checkProto2());
		
		List<FieldDescriptor> fields = b.getDescriptorForType().getFields();
		for(FieldDescriptor field : fields) {
			FieldType type = getFieldType(field);
			switch (type) {
			case Basic:
				this.addBasicField(field);
				break;
			case Blob:
				this.addBlobField(field);
				break;
			case Enum:
				this.addObjectField(field);

				break;
			case ID:
				this.setIdField(field);
				this.addBasicField(field);
				break;
			case Object:
				this.addObjectField(field);
				break;
			case RepeatedBasic:
				this.addRepeatedBasicField(field);
				break;
			case RepeatedEnum:
				this.addRepeatedObjectField(field);
				break;
			case RepeatedObject:
				this.addRepeatedObjectField(field);
				break;
			default:
				break;
			
			}
			
		}		
		
	}
	
	public FieldType getFieldType(FieldDescriptor field) {
		JavaType jType = field.getJavaType();
		
		if (field.getName().equalsIgnoreCase("ID"))
			return FieldType.ID;
		
		if (field.isRepeated())
		{
			if (jType == JavaType.MESSAGE 
//					|| jType == JavaType.BYTE_STRING
				) 
				return FieldType.RepeatedObject;		
			else if (jType == JavaType.ENUM) {
				return FieldType.RepeatedEnum;
			}
			else {
				return FieldType.RepeatedBasic;
			}

		}
		else {
			if (jType == JavaType.MESSAGE) {
				return FieldType.Object;
			}			
			else if (jType == JavaType.ENUM){
				return FieldType.Enum;
			}
			else if (jType == JavaType.BYTE_STRING)  {
				return FieldType.Blob;
			}
			else {
				return FieldType.Basic;
			}
		}
		
	}

	public String getBasicFieldName(FieldDescriptor field) {
		return field.getName().toLowerCase();
	}

	public String getObjectFieldName(FieldDescriptor field) {
		return "_" + getBasicFieldName(field) + "_ID";
	}

	public String getSaveStatement(Boolean objectExists) {
		String sql = StringUtils.EMPTY;
		
		if (!objectExists) {
			List<String> cols = getQuotedColumns();
			String[] params = new String[cols.size()];
			Arrays.fill(params, "?");
			
			sql = String.format(
				"INSERT INTO %s (%s) VALUES (%s)",
				this.getObjectName(),
				StringUtils.join(cols, ","),
				StringUtils.join(params, ","));
		}
		else {
			List<String> cols = getQuotedColumns();
			String updateCols = StringUtils.EMPTY;
			for (String col : cols) 
				updateCols += col + "=?,";
			updateCols = updateCols.substring(0, updateCols.length() -1);
					
			sql = String.format(
				"UPDATE %s SET %s WHERE ID = ?",
				this.getObjectName(),
				updateCols);
		}
		return sql;
	}

	protected List<String> getQuotedColumns() {
		List<String> cols = new ArrayList<String>();
		for (FieldDescriptor field : this.getObjectFields()) {
			cols.add(
				getQuotedColumn(
					getObjectFieldName(field)));
		}
		
		for (FieldDescriptor field : this.getBlobFields()) {
			cols.add(
				getQuotedColumn(
					getObjectFieldName(field)));
		}
		
		for (FieldDescriptor field : this.getBasicFields()) {
			String fieldName = field.getName();
			if (!fieldName.equalsIgnoreCase("ID"))
				cols.add(getQuotedColumn(fieldName));
		}
		
		return cols;
	}
	
	private String getQuotedColumn(String columnName) {
		return String.format("%s%s%s", 
				this.getBackend().getStartBracket(), 
				columnName,
				this.getBackend().getEndBracket());
	}
	
	public String getLinkTableInsertStatement(ProtoDBScanner other, String fieldName) {

		String sql = String.format(
				"INSERT INTO " + this.getLinkTableName(other, fieldName) + " ("
				+ "_" + this.getObjectName().toLowerCase() + "_ID,"
				+ "_" + other.getObjectName().toLowerCase() + "_ID"				
				+ ") VALUES (?, ?)");
		
		return sql;
	}	
	
	public String getLinkTableDeleteStatement(ProtoDBScanner other, String fieldName) {
		String sql = "DELETE FROM " + this.getLinkTableName(other, fieldName)
				+ " WHERE _" + this.getObjectName().toLowerCase() + "_ID = ?";
		return sql;
	}
	
	public String getBasicLinkTableDeleteStatement(FieldDescriptor field) {
		String sql = "DELETE FROM " + this.getBasicLinkTableName(field)
				+ " WHERE _" + this.getObjectName().toLowerCase() + "_ID = ?";
		
		return sql;
	}	
	
	public String getDeleteStatement() {
		return "DELETE FROM " + this.getObjectName() + " WHERE ID = ?";
	}
	
	public String getAddColumnStatement(FieldDescriptor field) {
		FieldType type = getFieldType(field);
		String columnDefinition = "ERROR";
		
		if (type == FieldType.Enum || type == FieldType.Object) {
			columnDefinition = getColumnDefinitionRef(field, false);
		}
		else if (type == FieldType.Basic) {
			columnDefinition = getColumnDefinitionBasic(field, false);
		}
		else if(type == FieldType.Blob) {
			columnDefinition = getColumnDefinitionBlob(field, false);
		}
		
		String sql = "ALTER TABLE %s ADD COLUMN %s ";
		
		return String.format(sql,
			this.getObjectName(),
			columnDefinition);
				
	}
	
	public String getCreateStatement() {
		String sql = String.format("CREATE TABLE %s ", this.getObjectName());
		List<String> cols = new ArrayList<String>();
		
		for(int i=0;i<this.getObjectFields().size();i++) {
			FieldDescriptor field = this.getObjectFields().get(i);
			cols.add(getColumnDefinitionRef(field, true));
		}
		
		for (FieldDescriptor field : this.getBlobFields()) {
			cols.add(getColumnDefinitionBlob(field, true));
		}
		
		for(FieldDescriptor field : this.getBasicFields()) {
			if (!field.getName().equalsIgnoreCase("ID"))
				cols.add(getColumnDefinitionBasic(field, true));
		}
		
		
		sql += String.format("(%s, %s)", this.getBackend().getIdentityDefinition(), StringUtils.join(cols, ","));
		
		return sql;
	}

	private String getColumnDefinitionBasic(FieldDescriptor field, boolean checkOptionality) {
		return String.format("%s %s %s", 
			getQuotedColumn(getBasicFieldName(field)), 
			getDBType(field), 
			field.isOptional() || !checkOptionality ? "NULL" : "NOT NULL");
	}

	private String getColumnDefinitionBlob(FieldDescriptor field, boolean checkOptionality) {
		return String.format("%s INTEGER %s REFERENCES BlobData (ID)",
				getQuotedColumn(getObjectFieldName(field)), 
				field.isOptional() || !checkOptionality ? "NULL" : "NOT NULL");
	}

	private String getColumnDefinitionRef(FieldDescriptor field, boolean checkOptionality) {
		String target = getObjectFieldTarget(field);
		return String.format("%s INTEGER %s REFERENCES %s (ID)",
				getQuotedColumn(getObjectFieldName(field)), 
				field.isOptional() || !checkOptionality ? "NULL" : "NOT NULL",
				target);
	}
	
	public String getLinkTableName(ProtoDBScanner other, String fieldName) {
		return this.getObjectName() + other.getObjectName() + "_" + StringUtils.capitalize(fieldName.replace("_", ""));
	}
	
	public String getBasicLinkTableName(FieldDescriptor field) {
		return this.getObjectName() + "_" + StringUtils.capitalize(field.getName().replace("_", ""));
	}
	
	public String getLinkCreateStatement(ProtoDBScanner other, String fieldName) {
		return String.format("CREATE TABLE %s ("
				+ "_" + this.getObjectName().toLowerCase() + "_ID INTEGER NOT NULL REFERENCES %s (ID),"
				+ "_" + other.getObjectName().toLowerCase() + "_ID INTEGER NOT NULL REFERENCES %s (ID)"
				+ ")", 
				this.getLinkTableName(other, fieldName), 
				this.getObjectName(),
				other.getObjectName());
		
	}
	
	public String getEnumLinkTableName(FieldDescriptor field) {
		return this.getObjectName() + field.getEnumType().getName() + "_" + StringUtils.capitalize(field.getName().replace("_", ""));
	}
	
	public String getEnumLinkCreateStatement(FieldDescriptor field) {
		return "CREATE TABLE " + getEnumLinkTableName(field) + "("
				+ "_" + this.getObjectName().toLowerCase() + "_ID INTEGER NOT NULL REFERENCES " + this.getObjectName() + " (ID),"
				+ "_" + field.getEnumType().getName().toLowerCase() + "_ID INTEGER NOT NULL REFERENCES " + field.getEnumType().getName() + " (ID)"
				+ ")"; 
	}
	
	
	public String getBasicLinkCreateStatement(FieldDescriptor field) {
		return String.format("CREATE TABLE %s ("
				+ "_" + this.getObjectName().toLowerCase() + "_ID INTEGER NOT NULL REFERENCES %s (ID),"
				+ "value %s NOT NULL"
				+ ")", 
				this.getBasicLinkTableName(field), 
				this.getObjectName(),
				this.getDBType(field));
		
	}	
	
	public String getSelectStatement(int id) {
		List<String> cols = getQuotedColumns();
		
		return "SELECT " + StringUtils.join(cols, ",") 
			+ " FROM " + this.getObjectName()
			+ " WHERE ID = ?";
	}	
	
	public String getSearchStatement(FieldDescriptor field, Boolean isLikeFilter) {
		return String.format(
				"SELECT ID FROM %s WHERE %s %s %s"
				, this.getObjectName()
				, this.getBasicFieldName(field)
				, (isLikeFilter ? " LIKE ? " : " = ?")
				, (isLikeFilter ? this.getBackend().getEscapeString() : ""));
	}
	
	public String getSearchStatementSubObject(FieldDescriptor field, List<Integer> subObjectIDs) {
		return "SELECT ID " 
			+ " FROM " + this.getObjectName()
			+ " WHERE " + this.getObjectFieldName(field) 
			+ " IN (" + StringUtils.join(subObjectIDs, ",") + ")";
	}	
	
	public String getSearchStatementLinkObject(FieldDescriptor field, ProtoDBScanner other, List<Integer> subObjectIDs) {
		return " SELECT A._" + this.getObjectName().toLowerCase() + "_ID AS ID"
				+  " FROM " + this.getLinkTableName(other, field.getName()) + " A"
				+  " WHERE A._" + other.getObjectName().toLowerCase() + "_ID "
				+  " IN (" + StringUtils.join(subObjectIDs, ",") + ")";
	}
	
	public String getSearchStatement(EnumDescriptor field) {
		return "SELECT ID"
				+ " FROM " + StringUtils.capitalize(field.getName())
				+ " WHERE value = ?";
	}			
	
	public String getLinkTableSelectStatement(ProtoDBScanner other, String fieldName) {
		return " SELECT A._" + other.getObjectName().toLowerCase() + "_ID AS ID"
			+  " FROM " + this.getLinkTableName(other, fieldName) + " A"
			+  " WHERE A._" + this.getObjectName().toLowerCase() + "_ID = ?";
	}

	public String getLinkTableSelectStatementIn(ProtoDBScanner other, String fieldName) {
		return " SELECT  A._" + this.getObjectName().toLowerCase() + "_ID AS thisID, "
		    +  " A._" + other.getObjectName().toLowerCase() + "_ID AS ID"
			+  " FROM " + this.getLinkTableName(other, fieldName) + " A"
			+  " WHERE A._" + this.getObjectName().toLowerCase() + "_ID IN (%s)";
	}

	public String getBasicLinkTableSelectStatement(FieldDescriptor field) {
		return " SELECT value FROM " + this.getBasicLinkTableName(field)
			+  " WHERE _" + this.getObjectName().toLowerCase() + "_ID = ?";
	}	

	public String getBasicLinkTableSelectStatementIn(FieldDescriptor field, List<Integer> ids) {
		return String.format(
				" SELECT %s_%s_ID%s, %1$svalue%3$s FROM %s WHERE %1$s_%2$s_ID%3$s IN (%s)"
			, this.getBackend().getStartBracket()
			, this.getObjectName().toLowerCase()
			, this.getBackend().getEndBracket()
			, this.getBasicLinkTableName(field)
			, StringUtils.join(ids, ","));
			
	}	


	public String getBasicLinkInsertStatement(FieldDescriptor field) {
		return String.format("INSERT INTO %s ("
				+ "_" + this.getObjectName().toLowerCase() + "_ID,"
				+ "value)"
				+ " VALUES (?,?)",
				this.getBasicLinkTableName(field));		
	}

	
	
	
	private String getDBType(FieldDescriptor field) {
		JavaType jType = field.getJavaType();
		String type = "TEXT";

		if (jType == JavaType.BOOLEAN)
			type = "BOOLEAN";
		else if (jType == JavaType.DOUBLE)
			type = "DOUBLE";
		else if (jType == JavaType.ENUM)
			type = "TINYINT";
		else if (jType == JavaType.FLOAT)
			type = "FLOAT";
		else if (jType == JavaType.INT)
			type = "INTEGER";
		else if (jType == JavaType.LONG)
			type = "BIGINT";
		else if (jType == JavaType.BYTE_STRING)
			type = "BLOB";
		else
			type ="TEXT";
		
		return type;
	}
	
	public PreparedStatement compileLinkBasicArguments(String sql, int thisID, JavaType jType, Object value, Connection conn) throws SQLException {
		PreparedStatement prep = conn.prepareStatement(sql);
		
		prep.setInt(1, thisID);
		
		compileArgument(2, prep, jType, value);
		
		return prep;
	}

	public PreparedStatement compileArguments(MessageOrBuilder b, String sql, Boolean objectExists, Connection conn) throws SQLException {
		PreparedStatement prep = conn.prepareStatement(sql);
		
		int c = 0;
		for(FieldDescriptor field : this.getObjectFields()) {
			Integer objectID = this.getObjectID(field.getName());
			if (objectID == null)
				throw new IllegalArgumentException(String.format("Failed to compile arguments. Reference ID for field %s not found",  field.getName()));
			
			prep.setInt(++c, objectID);
		}
		
		for (FieldDescriptor field : this.getBlobFields()) {
			Integer objectID = this.getBlobID(field.getName());
			if (objectID == null)
				throw new IllegalArgumentException(String.format("Failed to compile arguments. Blob ID for field %s not found",  field.getName()));
			
			prep.setInt(++c, this.getBlobID(field.getName()));
		}
		
		for(FieldDescriptor field : this.getBasicFields()) {
			if (!field.getName().equalsIgnoreCase("ID"))
				this.compileArgument(++c, prep, field.getJavaType(), b.getField(field));			
		}
		
		if (objectExists)
			prep.setInt(++c, this.getIdValue());
		
		return prep;
	}

	private void compileArgument(int i, PreparedStatement prep, JavaType jType, Object value) throws SQLException {
		if (jType == JavaType.BOOLEAN)
			prep.setBoolean(i, (boolean)value);
		else if (jType == JavaType.DOUBLE)
			prep.setDouble(i, (double)value);
		else if (jType == JavaType.ENUM)
			prep.setString(i, value.toString());
		else if (jType == JavaType.FLOAT)
			prep.setFloat(i, (float)value);
		else if (jType == JavaType.INT)
			prep.setInt(i, (int)value);
		else if (jType == JavaType.LONG)
			prep.setLong(i, (long)value);
		else if (jType == JavaType.BYTE_STRING)
			prep.setBytes(i, ((ByteString)value).toByteArray());
		else
			prep.setString(i, value.toString());
			
	}

//	public List<String> getFieldNames() {
//		return fieldNames;
//	}

	public Integer getObjectID(String fieldName) {
		return objectIDs.get(fieldName);
	}

	public void addObjectID(String fieldName, int id) {
		this.objectIDs.put(fieldName, id);
	}
	
	public Integer getBlobID(String fieldName) {
		return blobIDs.get(fieldName);
	}

	public void addBlobID(String fieldName, int id) {
		this.blobIDs.put(fieldName, id);
	}
	

	public List<FieldDescriptor> getObjectFields() {
		return objectFields;
	}

	public void addObjectField(FieldDescriptor field) {
		this.objectFields.add(field);
	}
	
	public List<FieldDescriptor> getBlobFields() {
		return blobFields;
	}

	public void addBlobField(FieldDescriptor field) {
		this.blobFields.add(field);
	}	

	public List<FieldDescriptor> getBasicFields() {
		return basicFields;
	}

	public void addBasicField(FieldDescriptor field) {
		this.basicFields.add(field);
	}

	public List<FieldDescriptor> getRepeatedObjectFields() {
		return repeatedObjectFields;
	}

	public void addRepeatedObjectField(FieldDescriptor field) {
		this.repeatedObjectFields.add(field);
	}

	public List<FieldDescriptor> getRepeatedBasicFields() {
		return repeatedBasicFields;
	}

	public void addRepeatedBasicField(FieldDescriptor field) {
		this.repeatedBasicFields.add(field);
	}

	public String getObjectName() {
		return objectName;
	}

	private void setObjectName(String objectName) {
		this.objectName = objectName;
	}
	
	public MessageOrBuilder getMessage() {
		return message;
	}

	private void setMessage(MessageOrBuilder message) {
		this.message = message;
	}
	
	public DatabaseBackend getBackend() {
		return backend;
	}

	public void setBackend(DatabaseBackend backend) {
		this.backend = backend;
	}


	public FieldDescriptor getIdField() {
		return idField;
	}

	private void setIdField(FieldDescriptor idField) {
		this.idField = idField;
	}
	
	public int getIdValue() {
		Object o = this.getMessage().getField(this.getIdField());
		return (int)o;
	}
	
	public FieldDescriptor getFieldByName(String fieldName) {
		for (FieldDescriptor field : this.getMessage().getDescriptorForType().getFields()) {
			if (StringUtils.equalsIgnoreCase(field.getName(), fieldName))
				return field;
		}
		
		return null;
	}
	
	public boolean checkProto2() {
		// get the first field (should always be one right?)
		FieldDescriptor f = this.getMessage().getDescriptorForType().getFields().get(0);
		String methodName = String.format("has%s", StringUtils.capitalize(f.getName()));
	
		// if the message has a has<fieldname> method then it is proto2 and not proto3
		Method[] methods = this.getMessage().getClass().getDeclaredMethods();
		
		for (Method meth : methods) {
			if (StringUtils.equalsIgnoreCase(meth.getName(), methodName) ) 
				return true;
		}
		return false;

	}


	public boolean isProto2() {
		return isProto2;
	}

	public void setProto2(boolean isProto2) {
		this.isProto2 = isProto2;
	}
	
	public String getObjectFieldTarget(FieldDescriptor field) {
		FieldType type = getFieldType(field);
		if (type == FieldType.Enum)
			return field.getEnumType().getName();
		
		if (type == FieldType.Object) {
			MessageOrBuilder target = (MessageOrBuilder)this.getMessage().getField(field);
			ProtoDBScanner dbInternal = new ProtoDBScanner(target, this.getBackend());
			
			return dbInternal.getObjectName();
		}

		return StringUtils.EMPTY;
	}

	public String getHashBlobSql() {
		/* SELECT 'object_id', 'field.getName()', ID, MD5(data) FROM object A INNER JOIN BlobData B 
		 SELECT A.*, MD5(B.data) FROM (
				 	SELECT 199, 'image', _image_ID AS _blob_ID FROM Movie WHERE ID = 199 
				 	UNION 
				 	SELECT 199, 'thumbnail', _thumbnail_id AS _blob_ID FROM Movie WHERE ID = 199
			 	 ) A
	 	 		 INNER JOIN BlobData B ON A._blob_ID = B.ID;
	 	 */
		List<String> rows = new ArrayList<String>();
		for (FieldDescriptor f : this.getBlobFields()) {
			rows.add(String.format("SELECT '%3$s' AS fieldName, %1$s%4$s%2$s AS _blob_ID FROM %1$s%5$s%2$s WHERE ID = %6$s",
					this.getBackend().getStartBracket(),
					this.getBackend().getEndBracket(),
					f.getName().toLowerCase(),
					getObjectFieldName(f),
					this.getObjectName(),
					this.getIdValue()));
		}
		String md5 = String.format(this.getBackend().getMD5Function(), "B.data");
		
		return String.format("SELECT A.fieldName, A._blob_ID, %3$s AS %1$s__hash__%2$s FROM (%4$s) A INNER JOIN BlobData B ON A._blob_ID = B.ID",
				this.getBackend().getStartBracket(),
				this.getBackend().getEndBracket(),
				md5,
				String.join(" UNION ", rows));
		
	}

	public String getUpdateBlobSql() {
		return "UPDATE BlobData SET data = ? WHERE ID = ?";
	}
}
