package se.qxx.protodb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Descriptors.FieldDescriptor;

public class ProtoDBScanner {

	String objectName;
	private MessageOrBuilder message = null;
	
	private List<FieldDescriptor> objectFields = new ArrayList<FieldDescriptor>();
	private List<String> objectFieldTargets = new ArrayList<String>();

	private List<FieldDescriptor> repeatedObjectFields = new ArrayList<FieldDescriptor>();
//	private List<String> repeatedObjectFieldTargets = new ArrayList<String>();
	
	private List<FieldDescriptor> basicFields = new ArrayList<FieldDescriptor>();
	private List<FieldDescriptor> repeatedBasicFields = new ArrayList<FieldDescriptor>();
	
	private List<FieldDescriptor> blobFields  = new ArrayList<FieldDescriptor>();
	
	FieldDescriptor idField = null;
	

	private HashMap<String, Integer> objectIDs = new HashMap<String,Integer>();
	private HashMap<String, Integer> blobIDs = new HashMap<String,Integer>();

	private HashMap<String, String> aliases = new HashMap<String, String>();
	
	public ProtoDBScanner(MessageOrBuilder b) {
		this.setMessage(b);
		this.scan(b);
	}
	
	private void scan(MessageOrBuilder b) {
			
		this.setObjectName(StringUtils.capitalize(b.getDescriptorForType().getName()));

		List<FieldDescriptor> fields = b.getDescriptorForType().getFields();
		for(FieldDescriptor field : fields) {
//			Object o = b.getField(field);
//			ProtoDBScanner dbInternal = null;
			JavaType jType = field.getJavaType();
			
			if (field.getName().equalsIgnoreCase("ID"))
				this.setIdField(field);
			
			if (field.isRepeated())
			{
				if (jType == JavaType.MESSAGE) 
					this.addRepeatedObjectField(field);		
				else if (jType == JavaType.ENUM) {
//					EnumValueDescriptor target = (EnumValueDescriptor)this.getMessage().getField(field);
					this.addRepeatedObjectField(field);
				}
				else {
					this.addRepeatedBasicField(field);
				}

			}
			else {
				if (jType == JavaType.MESSAGE) {
					MessageOrBuilder target = (MessageOrBuilder)this.getMessage().getField(field);
					ProtoDBScanner dbInternal = new ProtoDBScanner(target);			
					
					this.addObjectField(field);
					this.addObjectFieldTarget(dbInternal.getObjectName());
				}			
				else if (jType == JavaType.ENUM){					
					this.addObjectField(field);
					this.addObjectFieldTarget(field.getEnumType().getName());					
				}
				else if (jType == JavaType.BYTE_STRING)  {
					this.addBlobField(field);
				}
				else {
					this.addBasicField(field);
				}
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
			cols.add(String.format("[%s]", getObjectFieldName(field)));
		}
		
		for (FieldDescriptor field : this.getBlobFields()) {
			cols.add(String.format("[%s]", getObjectFieldName(field)));
		}
		
		for (FieldDescriptor field : this.getBasicFields()) {
			String fieldName = field.getName();
			if (!fieldName.equalsIgnoreCase("ID"))
				cols.add(String.format("[%s]", fieldName));
		}
		
		return cols;
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
				+ " WHERE _" + this.getObjectName().toLowerCase() + "_ID = ?"
				+ " AND _" + other.getObjectName().toLowerCase() + "_ID = ?";;
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
	
	public String getCreateStatement() {
		String sql = String.format("CREATE TABLE %s ", this.getObjectName());
		List<String> cols = new ArrayList<String>();
		
		for(int i=0;i<this.getObjectFields().size();i++) {
			FieldDescriptor field = this.getObjectFields().get(i);
			String target = this.getObjectFieldTargets().get(i);
			
			cols.add(String.format("[%s] INTEGER %s REFERENCES %s (ID)",
					getObjectFieldName(field), 
					field.isOptional() ? "NULL" : "NOT NULL",
					target));
		}
		
		for (FieldDescriptor field : this.getBlobFields()) {
			cols.add(String.format("[%s] INTEGER %s REFERENCES BlobData (ID)",
					getObjectFieldName(field), 
					field.isOptional() ? "NULL" : "NOT NULL"));
		}
		
		for(FieldDescriptor field : this.getBasicFields()) {
			if (!field.getName().equalsIgnoreCase("ID"))
				cols.add(String.format("[%s] %s %s", 
					getBasicFieldName(field), 
					getDBType(field), 
					field.isOptional() ? "NULL" : "NOT NULL"));
		}
		
		
		sql += "(ID INTEGER PRIMARY KEY AUTOINCREMENT, " + StringUtils.join(cols, ",") + ")";
		;
		return sql;
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
		return "SELECT ID " 
			+ " FROM " + this.getObjectName()
			+ " WHERE " + this.getBasicFieldName(field)
			+ (isLikeFilter ? " LIKE ? ESCAPE '\\'" : " = ?");
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
	
	public String getBasicLinkTableSelectStatement(FieldDescriptor field) {
		return " SELECT value FROM " + this.getBasicLinkTableName(field)
			+  " WHERE _" + this.getObjectName().toLowerCase() + "_ID = ?";
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

	private static void compileArgument(int i, PreparedStatement prep, JavaType jType, Object value) throws SQLException {
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

	public List<String> getObjectFieldTargets() {
		return objectFieldTargets;
	}

	public void addObjectFieldTarget(String target) {
		this.objectFieldTargets.add(target);
	}
	
//	public List<String> getRepeatedObjectFieldTargets() {
//		return repeatedObjectFieldTargets;
//	}
//
//	public void addRepeatedObjectFieldTarget(String target) {
//		this.repeatedObjectFieldTargets.add(target);
//	}	
	

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
	
	public JoinResult getJoinQuery(boolean getBlobs) {
		HashMap<String, String> aliases = new HashMap<String, String>();
		String currentAlias = "A";
		aliases.put(StringUtils.EMPTY, "A");

		String columnList = ProtoDBScanner.getColumnListForJoin(this, aliases, currentAlias, StringUtils.EMPTY, getBlobs);
		columnList = StringUtils.left(columnList, columnList.length() - 2);
		String joinList = ProtoDBScanner.getJoinClause(null, this, StringUtils.EMPTY, aliases, new MutableInt(1), StringUtils.EMPTY, StringUtils.EMPTY);
		
		String sql = String.format("SELECT %s FROM %s AS A %s"
				, columnList
				, this.getObjectName()
				, joinList);
		
		return new JoinResult(sql, aliases);
		 
	}
	
	private static String getJoinClauseRepeated(ProtoDBScanner parentScanner, ProtoDBScanner scanner, String parentFieldName, HashMap<String, String> aliases, MutableInt linkTableIterator, String parentHierarchy, String fieldHierarchy) {
		
		String joinClause = StringUtils.EMPTY;
					
		if (parentScanner != null) {
			joinClause += String.format("LEFT JOIN %s AS L%s ", 
					parentScanner.getLinkTableName(scanner, parentFieldName), 
					linkTableIterator);
			
			joinClause += String.format(" ON L%s._%s_ID = %s.ID ", 
					linkTableIterator, 
					parentScanner.getObjectName().toLowerCase(), 
					aliases.get(parentHierarchy));
			
			joinClause += String.format("LEFT JOIN %s AS %s ", 
					scanner.getObjectName(), 
					aliases.get(fieldHierarchy));
			
			joinClause += String.format(" ON L%s._%s_ID = %s.ID ", 
					linkTableIterator, 
					scanner.getObjectName().toLowerCase(), 
					aliases.get(fieldHierarchy));
			
			linkTableIterator.increment();
		}
		
		
		return joinClause;		
	}
	
	private static String getJoinClauseSimple(ProtoDBScanner parentScanner, ProtoDBScanner scanner, String parentFieldName, HashMap<String, String> aliases, String parentHierarchy, String fieldHierarchy) {
		String joinClause = StringUtils.EMPTY;
		
		if (parentScanner != null) {
			joinClause += String.format("LEFT JOIN %s AS %s ", 
				scanner.getObjectName(), 
				aliases.get(fieldHierarchy));
			
			joinClause += String.format(" ON %s._%s_ID = %s.ID ",
				aliases.get(parentHierarchy),
				parentFieldName.toLowerCase(),
				aliases.get(fieldHierarchy));
		}
		
		return joinClause;
	}
	
	public static String getJoinClause(ProtoDBScanner parentScanner, ProtoDBScanner scanner, String parentFieldName, HashMap<String, String> aliases, MutableInt linkTableIterator, String parentHierarchy, String fieldHierarchy) {
		String joinClause = StringUtils.EMPTY;
		
		for (FieldDescriptor f : scanner.getRepeatedObjectFields()) {
			DynamicMessage mg = DynamicMessage.getDefaultInstance(f.getMessageType());
			ProtoDBScanner other = new ProtoDBScanner(mg);
			String hierarchy = String.format("%s.%s", fieldHierarchy, f.getName());
			
			joinClause += getJoinClauseRepeated(scanner, other, f.getName(), aliases, linkTableIterator, fieldHierarchy, hierarchy);
			joinClause += getJoinClause(scanner, other, f.getName(), aliases, linkTableIterator, fieldHierarchy, hierarchy);
		}
		
		for (FieldDescriptor f : scanner.getObjectFields()) { 
			DynamicMessage mg = DynamicMessage.getDefaultInstance(f.getMessageType());
			ProtoDBScanner other = new ProtoDBScanner(mg);
			String hierarchy = String.format("%s.%s", fieldHierarchy, f.getName());
			
			joinClause += getJoinClauseSimple(scanner, other, f.getName(), aliases, fieldHierarchy, hierarchy);
			joinClause += getJoinClause(scanner, other, f.getName(), aliases, linkTableIterator, fieldHierarchy, hierarchy);
		}

		return joinClause;
	}
	

	public static String getColumnListForJoin(ProtoDBScanner scanner, HashMap<String, String> aliases, String currentAlias, String parentHierarchy, boolean getBlobs) {
		// the purpose of this is to create a sql query that joins all table together
		// Each column returned should have a previs with the object identity followed by
		// underscore and the column name. I.e. Object_field. This to avoid conflict with
		// each other on field names. All link tables and foreign key columns should be excluded.
		
		String columnList = StringUtils.EMPTY;
		
		for (FieldDescriptor b : scanner.getBasicFields()) {
			columnList += String.format("%s.[%s] AS %s_%s, ", currentAlias, b.getName(), currentAlias, b.getName()); 
		}
		
		int ac = 0;
		
		for (FieldDescriptor f : scanner.getObjectFields()) {
			String otherAlias = currentAlias + ((char)(65 + ac));
			
			DynamicMessage mg = DynamicMessage.getDefaultInstance(f.getMessageType());

			ProtoDBScanner other = new ProtoDBScanner(mg);
			String hierarchy = String.format("%s.%s", parentHierarchy, f.getName());
			aliases.put(hierarchy, otherAlias);

			columnList += ProtoDBScanner.getColumnListForJoin(other, aliases, otherAlias, hierarchy, getBlobs);
			
			ac++;
		}
		
		for (FieldDescriptor f : scanner.getRepeatedObjectFields()) {
			String otherAlias = currentAlias + ((char)(65 + ac));
			
			DynamicMessage mg = DynamicMessage.getDefaultInstance(f.getMessageType());

			ProtoDBScanner other = new ProtoDBScanner(mg);
			String hierarchy = String.format("%s.%s", parentHierarchy, f.getName());
			aliases.put(hierarchy, otherAlias);

			columnList += ProtoDBScanner.getColumnListForJoin(other, aliases, otherAlias, hierarchy, getBlobs);
			
			ac++;
		}		
		
		if (getBlobs) {
			//TODO!
			for (FieldDescriptor f : scanner.getBlobFields()) {
				String otherAlias = currentAlias + ((char)(65 + ac));
				
				ac++;	
			}
		}

//		this.getBasicFields();
//		this.getRepeatedBasicFields();
//		this.getRepeatedObjectFields();
//		this.getBlobFields()();
		
		return columnList;
	}

}
