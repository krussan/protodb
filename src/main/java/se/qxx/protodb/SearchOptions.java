package se.qxx.protodb;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.protobuf.Message;

import se.qxx.protodb.model.ProtoDBSearchOperator;

public class SearchOptions<T extends Message> {
	private T instance = null;
	private String fieldName;
	private ProtoDBSearchOperator operator = ProtoDBSearchOperator.Equals;
	private Object searchFor;
	private boolean shallowSearch = false;
	private List<String> excludedObjects = new ArrayList<String>();
	private int numberOfResults = -1;
	private int offset = -1;
	private String sortField = StringUtils.EMPTY;
	private ProtoDBSort sortOrder;
	
	public ProtoDBSort getSortOrder() {
		return sortOrder;
	}

	public SearchOptions<T> setSortOrder(ProtoDBSort sortOrder) {
		this.sortOrder = sortOrder;
		return this;
	}

	public String getSortField() {
		return sortField;
	}

	public SearchOptions<T> setSortField(String sortField) {
		this.sortField = sortField;
		return this;
	}

	public int getOffset() {
		return offset;
	}

	public SearchOptions<T> setOffset(int offset) {
		this.offset = offset;
		return this;
	}

	public int getNumberOfResults() {
		return numberOfResults;
	}

	public SearchOptions<T> setNumberOfResults(int numberOfResults) {
		this.numberOfResults = numberOfResults;
		return this;
	}

	public List<String> getExcludedObjects() {
		return excludedObjects;
	}

	public boolean isShallowSearch() {
		return shallowSearch;
	}

	private void setShallowSearch(boolean shallowSearch) {
		this.shallowSearch = shallowSearch;
	}

	public ProtoDBSearchOperator getOperator() {
		return operator;
	}

	private void setOperator(ProtoDBSearchOperator operator) {
		this.operator = operator;
	}

	public String getFieldName() {
		return fieldName;
	}

	private void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	private SearchOptions(T instance) {
		this.setInstance(instance);
	}

	public static <T extends Message>SearchOptions<T> newBuilder(T instance) {
		return new SearchOptions<T>(instance);
	}

	public T getInstance() {
		return instance;
	}

	private void setInstance(T instance) {
		this.instance = instance;
	}
	
	public SearchOptions<T> addFieldName(String fieldName) {
		this.setFieldName(fieldName);
		return this;
	}
	
	public SearchOptions<T> addOperator(ProtoDBSearchOperator operator) {
		this.setOperator(operator);
		return this;
	}
	
	public SearchOptions<T> addSearchArgument(Object value) {
		this.setSearchFor(value);
		return this;
	}
	
	public SearchOptions<T> setShallow(boolean shallow) {
		this.setShallowSearch(shallow);
		return this;
	}
	
	public SearchOptions<T> addExcludedObject(String object) {
		this.getExcludedObjects().add(object);
		return this;
	}

	public Object getSearchFor() {
		return searchFor;
	}

	private void setSearchFor(Object searchFor) {
		this.searchFor = searchFor;
	}

	public boolean validate() {
		return (this.getInstance() != null);
	}
}