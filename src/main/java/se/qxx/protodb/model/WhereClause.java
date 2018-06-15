package se.qxx.protodb.model;

import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

import se.qxx.protodb.ProtoDBScanner;
import se.qxx.protodb.exceptions.SearchFieldNotFoundException;

public class WhereClause {
	SearchField searchField;
	ProtoDBSearchOperator operator;
	Object value;

	public SearchField getSearchField() {
		return searchField;
	}

	public void setSearchField(SearchField searchField) {
		this.searchField = searchField;
	}

	public ProtoDBSearchOperator getOperator() {
		return operator;
	}

	public void setOperator(ProtoDBSearchOperator operator) {
		this.operator = operator;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	private WhereClause(SearchField searchField, ProtoDBSearchOperator operator, Object value) {
		this.searchField = searchField;
		this.operator = operator;
		this.value = value;
	}
	
	public static WhereClause create(
			ProtoDBScanner scanner, 
			HashMap<String, String> aliases, 
			String fieldName, 
			ProtoDBSearchOperator operator, 
			Object value) throws SearchFieldNotFoundException {
		
		if (!StringUtils.isEmpty(fieldName)) {
			SearchField field = SearchField.find(scanner, aliases, fieldName);
			
			if (field != null)
				return new WhereClause(field, operator, value);
		}	
		
		return null;
	}
	
	public static WhereClause createLink(ProtoDBScanner other, List<Integer> listOfIds) {
		SearchField field = new SearchField(
				String.format("_%s_ID", other.getObjectName().toLowerCase()),
				"L0");

		return new WhereClause(field, ProtoDBSearchOperator.In, StringUtils.join(listOfIds, ","));
		
//		this.getWhereClauses().add(
//				String.format("L0.%s_%s_ID%s IN (%s)",
//					other.getBackend().getStartBracket(),
//					other.getObjectName().toLowerCase(),
//					other.getBackend().getEndBracket(),
//					listOfIds));						
	}
	


	public String getSql() {
		
		if (this.getOperator() == ProtoDBSearchOperator.In) {
			return String.format(
					"%s IN (%s)", 
					this.getSearchField().getAliasFieldName(),
					value);				
		}
		else {
			return String.format("%s %s ?", 
						this.getSearchField().getAliasFieldName(),
						(this.getOperator() == ProtoDBSearchOperator.Like ? "LIKE" : "="));
		}
	}
	
}
