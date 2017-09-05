package se.qxx.protodb.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

import se.qxx.protodb.ProtoDBScanner;

public class ProtoField {
	private ProtoType type;
	private String name;
	private ProtoTable objectOrRepeatedTable;
	private SearchArgument searchArgument;
	private boolean isIdField = false;
	private boolean isOptional = false;
	private String dbType;
	private String enumType;
	
	public String getEnumType() {
		return enumType;
	}

	public void setEnumType(String enumType) {
		this.enumType = enumType;
	}

	public String getDbType() {
		return dbType;
	}

	public void setDbType(String dbType) {
		this.dbType = dbType;
	}

	public ProtoField(MessageOrBuilder b, FieldDescriptor field, String alias) {
		this.init(b, field, alias);
	}
	
	public ProtoType getType() {
		return type;
	}
	public void setType(ProtoType type) {
		this.type = type;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public ProtoTable getObjectOrRepeatedTable() {
		return objectOrRepeatedTable;
	}
	public void setObjectOrRepeatedTable(ProtoTable repeatedTable) {
		this.objectOrRepeatedTable = repeatedTable;
	}
	public SearchArgument getSearchArgument() {
		return searchArgument;
	}
	public void setSearchArgument(SearchArgument searchArgument) {
		this.searchArgument = searchArgument;
	}

	public boolean isIdField() {
		return isIdField;
	}

	public void setIdField(boolean isIdField) {
		this.isIdField = isIdField;
	}

	public boolean isOptional() {
		return isOptional;
	}

	public void setOptional(boolean isOptional) {
		this.isOptional = isOptional;
	}

	private void init(MessageOrBuilder b, FieldDescriptor field, String alias) {
		this.setName(field.getName());
		this.setOptional(field.isOptional());
		this.parseDbType(field);
		
		JavaType jType = field.getJavaType();
		
		if (field.getName().equalsIgnoreCase("ID"))
			this.setIdField(true);
		
		if (field.isRepeated())
			initRepeated(b, field, alias);
		else if (jType == JavaType.MESSAGE)
			initObject(ProtoType.Object, b, field, alias);
		else if(jType == JavaType.ENUM)
			initEnum(b, field);
	}
	
	private void parseDbType(FieldDescriptor field) {
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
		
		this.setDbType(type);

	}
	
	private void initEnum(MessageOrBuilder b, FieldDescriptor field) {
		this.setEnumType(field.getEnumType().getName());
	}

	private void initRepeated(MessageOrBuilder b, FieldDescriptor field, String alias) {
		JavaType javaType = field.getJavaType();
		
		if (javaType == JavaType.MESSAGE)
			initObject(ProtoType.RepeatedObject, b, field, alias);
		else if (javaType == JavaType.ENUM)
			initObject(ProtoType.Enum, b, field, alias);
		else
			initObject(ProtoType.RepeatedBasic, b, field, alias);
	}

	private void initObject(ProtoType type, MessageOrBuilder b, FieldDescriptor field, String alias) {
		this.setType(type);
		ProtoTable t = new ProtoTable(b, alias);
		this.setObjectOrRepeatedTable(t);
	}
	
	public String getBasicFieldName() {
		return this.getName().toLowerCase();
	}
	
	public String getObjectFieldName() {
		return "_" + this.getBasicFieldName() + "_ID";
	}

	public String getLinkTableInsertSql(ProtoTable parent) {

		return String.format(
				"INSERT INTO %s (%s, %s) VALUES (?, ?)"
				, this.getLinkTableName(parent)
				, getLinkField(parent)
				, getLinkField(this.getObjectOrRepeatedTable()));
		
	}

	public String getLinkTableName(ProtoTable parent) {
		return parent.getName() + this.getObjectOrRepeatedTable().getName() + "_" + StringUtils.capitalize(this.getName().replace("_", ""));
	}
	
	public String getLinkTableDeleteStatement(ProtoTable parent) {
		
		return String.format(
				"DELETE FROM %s WHERE %s = ? AND %s = ?" 
				, this.getLinkTableName(parent)
				, this.getLinkField(parent)
				, this.getLinkField(this.getObjectOrRepeatedTable()));
		
	}

	private String getLinkField(ProtoTable target) {
		return formatLinkField(target.getName());
	}
	
	private String formatLinkField(String columnName) {
		return String.format("_%s_ID", columnName.toLowerCase());
	}
	
	public String getBasicLinkTableDeleteStatement(ProtoTable parent) {
		return String.format("DELETE FROM %s WHERE %s = ?"
				, parent.getName()
				, getLinkTableName(parent));
	}	

	public String getBasicLinkTableName(ProtoTable parent) {
		return String.format("%s_%s"
				, parent.getName()
				, StringUtils.capitalize(this.getName().replace("_", "")));
	}
	
	public String getFieldCreateSql() {
		if (this.getType() == ProtoType.Object) {
			return String.format("[%s] INTEGER %s REFERENCES %s (ID)",
					getObjectFieldName(), 
					this.isOptional() ? "NULL" : "NOT NULL",
					this.getObjectOrRepeatedTable().getName());
			
		}
		
		if (this.getType() == ProtoType.Blob) {
			return String.format("[%s] INTEGER %s REFERENCES BlobData (ID)",
					this.getObjectFieldName(), 
					this.isOptional() ? "NULL" : "NOT NULL");
		}
		
		if (this.getType() == ProtoType.Basic) {
			return String.format("[%s] %s %s", 
					this.getBasicFieldName(), 
					this.getDbType(), 
					this.isOptional() ? "NULL" : "NOT NULL");			
		}
		
		return StringUtils.EMPTY;
	}

	public String getLinkCreateSql(ProtoTable parent) {
		return String.format("CREATE TABLE %s ("
				+ "%s INTEGER NOT NULL REFERENCES %s (ID),"
				+ "%s INTEGER NOT NULL REFERENCES %s (ID)"
				+ ")",
				this.getLinkTableName(parent),
				this.getLinkField(parent),
				parent.getName(),
				this.getLinkField(this.getObjectOrRepeatedTable()),
				this.getObjectOrRepeatedTable().getName());
		
	}

	public String getEnumLinkTableName(ProtoTable parent) {
		return String.format("%s%s_%s"
				, parent.getName()
				, this.getEnumType()
				, StringUtils.capitalize(this.getName().replace("_", "")));
	}
	
	public String getEnumLinkCreateSql(ProtoTable parent) {
		return String.format("CREATE TABLE %s (" 
				+ "%s INTEGER NOT NULL REFERENCES %s (ID),"
				+ "%s INTEGER NOT NULL REFERENCES %s (ID)"
				+ ")"
				, this.getEnumLinkTableName(parent)
				, this.getLinkField(parent)
				, parent.getName()
				, this.formatLinkField(this.getEnumType())
				, this.getEnumType());
	}
	
	public String getBasicLinkCreateSql(ProtoTable parent) {
		return String.format("CREATE TABLE %s ("
				+ "%s INTEGER NOT NULL REFERENCES %s (ID),"
				+ "value %s NOT NULL"
				+ ")", 
				this.getBasicLinkTableName(parent),
				this.getLinkField(parent),
				parent.getName(),
				this.getDbType());
		
	}	
	

	
}
