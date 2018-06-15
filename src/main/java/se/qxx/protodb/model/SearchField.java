package se.qxx.protodb.model;

import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

import se.qxx.protodb.ProtoDBScanner;
import se.qxx.protodb.exceptions.SearchFieldNotFoundException;

public class SearchField {
	private String fieldName;
	private String alias;
	
	public String getFieldName() {
		return fieldName;
	}
	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}
	public String getAlias() {
		return alias;
	}
	public void setAlias(String alias) {
		this.alias = alias;
	}
	
	public SearchField(String fieldName, String alias) {
		this.setFieldName(fieldName);
		this.setAlias(alias);
	}
	
	public String getAliasFieldName() {
		return String.format("%s.%s", this.getAlias(), this.getFieldName());
	}
	
	public static SearchField find(
			ProtoDBScanner scanner, 
			CaseInsensitiveMap aliases, 
			String searchField) throws SearchFieldNotFoundException {
		
		boolean isRootField = !StringUtils.contains(searchField, ".");
		FieldDescriptor whereField = getWhereField(scanner, searchField);
		boolean isEnumField = whereField.getJavaType() == JavaType.ENUM;
		
		String alias = StringUtils.EMPTY;
		String field = StringUtils.EMPTY;
		
		if (isEnumField) {
			String key = "." + searchField;
			field = "value";
			alias = aliases.get(key);				
		}
		else if (isRootField) {
			alias = "A";
			field = searchField;
		}
		else {
			String key = "." + StringUtils.substringBeforeLast(searchField, ".");
			field = StringUtils.substringAfterLast(searchField, ".");
			alias = aliases.get(key);
		}
		
		return new SearchField(field, alias);

	}
	
	private static FieldDescriptor getWhereField(ProtoDBScanner scanner, String searchField) throws SearchFieldNotFoundException {
		// We need to get the last field in the sequence and check if that field is an enum field
		boolean isRootField = !StringUtils.contains(searchField, ".");
		
		if (isRootField) {
			FieldDescriptor f = scanner.getFieldByName(searchField);
			if (f == null)
				throw new SearchFieldNotFoundException(searchField, scanner.getObjectName());
			
			return f;
			
		} else {
			String nextField = StringUtils.substringBefore(searchField, ".");
			String tail = StringUtils.substringAfter(searchField, ".");
			
			FieldDescriptor nf = scanner.getFieldByName(nextField);
			DynamicMessage obj = DynamicMessage.getDefaultInstance(nf.getMessageType());
			ProtoDBScanner other = new ProtoDBScanner(obj, scanner.getBackend());
			
			return getWhereField(other, tail);

		}
	}

}
