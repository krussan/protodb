package se.qxx.protodb.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

public class ProtoTable {
	private String name;
	private List<ProtoField> fields = new ArrayList<ProtoField>();
	private String alias;
	
	public ProtoTable(MessageOrBuilder b, String currentAlias) {
		this.setAlias(currentAlias);
		this.setName(StringUtils.capitalize(b.getDescriptorForType().getName()));
		this.init(b, currentAlias);
	}
	
	public ProtoTable(String name) {
		this.setName(name);
	}
	
	public ProtoTable(String name, String alias) {
		this.setName(name);
		this.setAlias(alias);
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<ProtoField> getFields() {
		return fields;
	}
	private void addField(ProtoField field) {
		this.fields.add(field);
	}	
	private void setFields(List<ProtoField> fields) {
		this.fields = fields;
	}
	public String getAlias() {
		return alias;
	}
	public void setAlias(String alias) {
		this.alias = alias;
	}
	
	private void init(MessageOrBuilder b, String currentAlias) {
		List<FieldDescriptor> fields = b.getDescriptorForType().getFields();
		
		int ac = 0;
		String otherAlias = currentAlias;

		for(FieldDescriptor field : fields) {
			if (field.getJavaType() == JavaType.MESSAGE) {
				ac++;
				otherAlias = currentAlias + ((char)(65 + ac));
			}
			
			this.addField(new ProtoField(b, field, otherAlias));
		}
	}
	
	public String getSaveSql(boolean objectExists) {
		String sql = StringUtils.EMPTY;
		
		if (!objectExists) {
			List<String> cols = getQuotedColumns();
			String[] params = new String[cols.size()];
			Arrays.fill(params, "?");
			
			sql = String.format(
				"INSERT INTO %s (%s) VALUES (%s)",
				this.getName(),
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
				this.getName(),
				updateCols);
		}
		return sql;
		
	}
	
	protected List<String> getQuotedColumns() {
		List<String> cols = new ArrayList<String>();
		for (ProtoField f : this.getFields()) {
			
			if (f.getType() == ProtoType.Object || 
					f.getType() == ProtoType.Blob) {
				
				cols.add(String.format("[%s]", f.getObjectFieldName()));
			}
			
			if (f.getType() == ProtoType.Basic && !f.isIdField()) {
				cols.add(String.format("[%s]", f.getName()));
			}
				
		}
		
		return cols;
	}
	
	public String getDeleteSql() {
		return String.format("DELETE FROM %s WHERE ID = ?", this.getName());
	}

	public String getCreateSql() {
		List<String> cols = new ArrayList<String>();
		
		for (ProtoField field : this.getFields()) {
			cols.add(field.getFieldCreateSql());
		}
		
		return String.format("CREATE TABLE %s (ID INTEGER PRIMARY KEY AUTOINCREMENT, %s)"
				, this.getName()
				, StringUtils.join(cols, ","));
	}

	public String getSelectSql(int id) {
		List<String> cols = getQuotedColumns();
		
		return "SELECT " + StringUtils.join(cols, ",") 
			+ " FROM " + this.getName()
			+ " WHERE ID = ?";
	}
	

}
