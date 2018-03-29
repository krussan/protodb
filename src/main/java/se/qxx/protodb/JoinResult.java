package se.qxx.protodb;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;

import se.qxx.protodb.backend.DatabaseBackend;
import se.qxx.protodb.exceptions.ProtoDBParserException;
import se.qxx.protodb.exceptions.SearchFieldNotFoundException;
import se.qxx.protodb.model.ProtoDBSearchOperator;

public class JoinResult {

	private HashMap<String, String> aliases = new HashMap<String,String>();
	private String joinClause = StringUtils.EMPTY;
	private List<String> whereClauses = new ArrayList<String>();
	private List<Object> whereParameters = new ArrayList<Object>();
	private boolean hasComplexJoins = false;
	private DatabaseBackend backend = null;
	private String sortSql = StringUtils.EMPTY;

	int nrOfResults = 0;
	int offset = 0;
	
	
	public int getNrOfResults() {
		return nrOfResults;
	}

	public void setNrOfResults(int nrOfResults) {
		this.nrOfResults = nrOfResults;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public boolean hasComplexJoins() {
		return hasComplexJoins;
	}

	public void setComplexJoins(boolean hasComplexJoins) {
		this.hasComplexJoins = hasComplexJoins;
	}

	public DatabaseBackend getBackend() {
		return backend;
	}

	public void setBackend(DatabaseBackend backend) {
		this.backend = backend;
	}
	
	public String getSortSql() {
		return sortSql;
	}

	public void setSortSql(String sortSql) {
		this.sortSql = sortSql;
	}

	public JoinResult(String joinClause, HashMap<String, String> aliases, boolean hasComplexJoins, int nrOfResults, int offset, DatabaseBackend backend) {
		this.setAliases(aliases);
		this.setJoinClause(joinClause);
		this.setComplexJoins(hasComplexJoins);
		this.setNrOfResults(nrOfResults);
		this.setOffset(offset);
		this.setBackend(backend);
	}

	public HashMap<String, String> getAliases() {
		return aliases;
	}

	private void setAliases(HashMap<String, String> aliases) {
		this.aliases = aliases;
	}

	public String getJoinClause() {
		return joinClause;
	}

	public void setJoinClause(String joinClause) {
		this.joinClause = joinClause;
	}
	
	public String getWhereClause() {
		if (this.getWhereClauses().isEmpty())
			return StringUtils.EMPTY;
		else
			return String.format(" WHERE %s",StringUtils.join(this.getWhereClauses(), " AND "));
	}
	
	private List<String> getWhereClauses() {
		return whereClauses;
	}

	private List<Object> getWhereParameters() {
		return whereParameters;
	}

	public void addLinkWhereClause(List<Integer> parentIDs, ProtoDBScanner other) {
		String listOfIds = StringUtils.join(parentIDs, ",");
		
		this.getWhereClauses().add(String.format("L0._" + other.getObjectName().toLowerCase() + "_ID IN (%s)", 
				listOfIds));						
	}
	
	public FieldDescriptor getWhereField(ProtoDBScanner scanner, String searchField) throws SearchFieldNotFoundException {
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
	
	public void addWhereClause(ProtoDBScanner scanner, String searchField, Object value, ProtoDBSearchOperator op) throws SearchFieldNotFoundException {
		if (!StringUtils.isEmpty(searchField)) {

			SearchField field = findSearchField(scanner, searchField);
			
			if (op == ProtoDBSearchOperator.In) {
				this.getWhereClauses().add(
						String.format("%s IN (%s)", 
								field.getAliasFieldName(),
								value));				
			}
			else {
				this.getWhereClauses().add(
						String.format("%s %s ?", 
								field.getAliasFieldName(),
								(op == ProtoDBSearchOperator.Like ? "LIKE" : "=")));
				
				this.getWhereParameters().add(value);
			}
			
		}
	}
	
	public void addSortOrder(ProtoDBScanner scanner, String sortField, ProtoDBSort sortOrder) throws SearchFieldNotFoundException {
		if (!StringUtils.isEmpty(sortField)) {
			SearchField field = findSearchField(scanner, sortField);
			this.setSortSql(getSortClause(field.getAliasFieldName(), sortOrder));
		}
	}


	private String getSortClause(String sortField, ProtoDBSort sortOrder) {
		if (!StringUtils.isEmpty(sortField)) {
			return String.format(
					" ORDER BY %s %s "
					, sortField
					, sortOrder == ProtoDBSort.Desc ? "DESC" : "ASC");
		}
		
		return "";
	}

	
	public SearchField findSearchField(ProtoDBScanner scanner, String searchField) throws SearchFieldNotFoundException {
		boolean isRootField = !StringUtils.contains(searchField, ".");
		FieldDescriptor whereField = getWhereField(scanner, searchField);
		boolean isEnumField = whereField.getJavaType() == JavaType.ENUM;
		
		String alias = StringUtils.EMPTY;
		String field = StringUtils.EMPTY;
		
		if (isEnumField) {
			String key = "." + searchField;
			field = "value";
			alias = this.getAliases().get(key);				
		}
		else if (isRootField) {
			alias = "A";
			field = searchField;
		}
		else {
			String key = "." + StringUtils.substringBeforeLast(searchField, ".");
			field = StringUtils.substringAfterLast(searchField, ".");
			alias = this.getAliases().get(key);
		}
		
		return new SearchField(field, alias);

	}
	
	public String getSql() {
		String sql = String.format("%s %s",this.getJoinClause(), this.getWhereClause());
		
		if (!StringUtils.isEmpty(this.getSortSql()))
			sql += this.getSortSql();
		
		if (this.getNrOfResults() > 0) {
			sql += String.format(" LIMIT %s ", this.getNrOfResults());
			if (this.getOffset() > 0) {
				sql += String.format("OFFSET %s", this.getOffset());
			}
		}
		
		
		
		return sql;
	}
	
	public PreparedStatement getStatement(Connection conn) throws SQLException {
		PreparedStatement prep = conn.prepareStatement(this.getSql());
		
		for(int i = 0; i<this.getWhereParameters().size(); i++) {
			Object o = this.getWhereParameters().get(i);
			prep.setObject(i + 1, o);
		}
		
		return prep;
	}
	
	public <T extends Message> Map<Integer, List<Object>> getResultLink(T instance, ResultSet rs, boolean getBlobs, List<String> excludedObjects) throws SQLException, ProtoDBParserException {
		Map<Integer, List<Object>> map = new HashMap<Integer, List<Object>>();
		
		while (rs.next()) {
			int parentID = rs.getInt("__thisID");
			
			if (!map.containsKey(parentID)) {
				map.put(parentID, new ArrayList<Object>()); 
			}
			
			map.get(parentID).add(getResult(instance, rs, StringUtils.EMPTY, getBlobs, excludedObjects));
		}
		
		return map;
	}
		
	public <T extends Message> List<T> getResult(T instance, ResultSet rs, boolean getBlobs, List<String> excludedObjects) throws SQLException, ProtoDBParserException {
		List<T> result = new ArrayList<T>();
		while (rs.next()) {
			result.add(getResult(instance, rs, StringUtils.EMPTY, getBlobs, excludedObjects));
		}
		
		return result;
	}

	@SuppressWarnings("unchecked")
	private <T extends Message> T getResult(T instance, ResultSet rs, String parentHierarchy, boolean getBlobs, List<String> excludedObjects) throws SQLException, ProtoDBParserException {
//		Builder b = instance.newBuilderForType();
		
		DynamicMessage.Builder b = DynamicMessage.newBuilder(instance.getDefaultInstanceForType());
		
//		ProtoDBScanner scanner = new ProtoDBScanner(instance);
//		Log(String.format("Populating object %s :: %s", scanner.getObjectName(), id));
//		
//		// populate list of sub objects
//		populateRepeatedObjectFields(id, excludedObjects, conn, b, scanner);
//
//		// populate list of basic types
//		populateRepeatedBasicFields(id, conn, b, scanner);
		
		ProtoDBScanner scanner = new ProtoDBScanner(instance, backend);
		
		for (FieldDescriptor f : scanner.getBasicFields()) {
			String alias = this.getAliases().get(parentHierarchy);
			String columnName = String.format("%s_%s", alias, f.getName());
			
			Object o = rs.getObject(columnName);
			Populator.populateField(b, f, o);
		}
		
		for (FieldDescriptor f : scanner.getObjectFields()) {
			if (!Populator.isExcludedField(f.getName(), excludedObjects)) {
				if (f.getJavaType() == JavaType.ENUM) {
					String alias = this.getAliases().get(parentHierarchy);
					String columnName = String.format("%s_%s", alias, f.getName());
					String enumValue = rs.getString(columnName);
					
					Populator.populateField(b, f, enumValue);
				}
				else {
					DynamicMessage mg = DynamicMessage.getDefaultInstance(f.getMessageType());
					String hierarchy = String.format("%s.%s", parentHierarchy, f.getName());
					
					mg = getResult(
							mg, 
							rs, 
							hierarchy, 
							getBlobs, 
							Populator.stripExcludedFields(
									f.getName(), 
									excludedObjects));
					
					b.setField(f, mg);
				}
			}
		}
		
		if (getBlobs) {
			for (FieldDescriptor f : scanner.getBlobFields()) {
				if (!Populator.isExcludedField(f.getName(), excludedObjects)) {
					String alias = this.getAliases().get(parentHierarchy);
					String columnName = String.format("%s_%s", alias, f.getName());
					
					byte[] byteData = rs.getBytes(columnName);
					
					Populator.populateField(b, f, byteData);
				}
			}
		}
		else {
			Populator.populateRequiredBlobs(b, scanner);
		}
		
		// invoke parseFrom by reflection to cast this from dynamicMessage to the actual type
		if (instance instanceof DynamicMessage)
			return (T) b.build();
		else
			return (T) constructTargetMessage(instance, b);
	}

	private <T extends Message> Object constructTargetMessage(T instance, DynamicMessage.Builder b)
			throws ProtoDBParserException {
		try {
			Method parseMethod = instance.getClass().getMethod("parseFrom", ByteString.class);
			Object o = parseMethod.invoke(null, b.build().toByteString());
			return o;			
		}
		catch (Exception e) {
			throw new ProtoDBParserException(instance.getClass().getName(), e);
		}
	}
	
	


}
