package se.qxx.protodb;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
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
import se.qxx.protodb.model.CaseInsensitiveMap;
import se.qxx.protodb.model.Column;
import se.qxx.protodb.model.ColumnResult;
import se.qxx.protodb.model.ProtoDBSearchOperator;
import se.qxx.protodb.model.SearchField;
import se.qxx.protodb.model.WhereClause;

public class JoinResult {

	private CaseInsensitiveMap aliases = new CaseInsensitiveMap();
	private List<JoinRow> joinClause = new ArrayList<JoinRow>();
	private List<WhereClause> whereClauses = new ArrayList<WhereClause>();
	
	private boolean hasComplexJoins = false;
	private DatabaseBackend backend = null;
	private String sortSql = StringUtils.EMPTY;
	private ColumnResult columnResult;

	int nrOfResults = 0;
	int offset = 0;

	public ColumnResult getColumnResult() {
		return columnResult;
	}

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
	
	public void setColumnResult(ColumnResult columnResult) {
		this.columnResult = columnResult;
	}

	public JoinResult(
			List<JoinRow> joinClause,
			ColumnResult columnResult,
			CaseInsensitiveMap aliases, 
			boolean hasComplexJoins, 
			int nrOfResults, 
			int offset,
			DatabaseBackend backend) {
		
		this.setAliases(aliases);
		this.setJoinClause(joinClause);
		this.setComplexJoins(hasComplexJoins);
		this.setNrOfResults(nrOfResults);
		this.setOffset(offset);
		this.setBackend(backend);
		this.setColumnResult(columnResult);
	}

	public CaseInsensitiveMap getAliases() {
		return aliases;
	}

	private void setAliases(CaseInsensitiveMap aliases) {
		this.aliases = aliases;
	}

	public List<JoinRow> getJoinClause() {
		return joinClause;
	}

	public void setJoinClause(List<JoinRow> joinClause) {
		this.joinClause = joinClause;
	}
	
	public String getWhereClause() {
		if (this.getWhereClauses().isEmpty())
			return StringUtils.EMPTY;
		else {
			StringBuilder sb = new StringBuilder(" WHERE ");
			for (WhereClause wc : this.getWhereClauses() ) {
				sb.append(String.format("%s AND ", wc.getSql()));
			}
			
			String sql = sb.toString();
			return StringUtils.left(sql, sql.length() - 4);
		}
	}
	
	private List<WhereClause> getWhereClauses() {
		return whereClauses;
	}

//	private List<Object> getWhereParameters() {
//		return whereParameters;
//	}

	public void addLinkWhereClause(List<Integer> parentIDs, ProtoDBScanner other) {
		this.getWhereClauses().add(
				WhereClause.createLink(other, parentIDs));
	}
	
	
	public void addWhereClause(ProtoDBScanner scanner, String searchField, Object value, ProtoDBSearchOperator op) throws SearchFieldNotFoundException {
		if (!StringUtils.isEmpty(searchField)) {
			WhereClause clause = 
					WhereClause.create(
							scanner, 
							this.getAliases(),
							searchField.toLowerCase(), 
							op, 
							value);
			
			if (clause != null)
				this.getWhereClauses().add(clause);
			
		}
	}
	
	public void addSortOrder(ProtoDBScanner scanner, String sortField, ProtoDBSort sortOrder) throws SearchFieldNotFoundException {
		if (!StringUtils.isEmpty(sortField)) {
			SearchField field = SearchField.find(scanner, this.getAliases(), sortField);
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

		
	private String getJoinSql() {
		String joinSql = StringUtils.EMPTY;
		for (JoinRow row : this.getJoinClause()) {
			if (row.isIncluded())
				joinSql += row.getJoinCluase();
		}
		
		return joinSql;
	}
	
	private String getResultSql() {
		String sql = String.format("SELECT %s%s %s"
				, this.hasComplexJoins() ? "DISTINCT " : ""
				, this.getColumnResult().getSql()
				, this.getJoinSql());

		return sql;
	}
	
	public String getSql() {
	
		// Filter the joinclauses
		filterJoins();
		
		// create output
		String sql = String.format("%s %s",this.getResultSql(), this.getWhereClause());
		
		if (!StringUtils.isEmpty(this.getSortSql()))
			sql += this.getSortSql();
		
		if (this.getNrOfResults() > 0) {
			sql += String.format(" LIMIT %s ", this.getNrOfResults());
			if (this.getOffset() > 0) {
				sql += String.format("OFFSET %s", this.getOffset());
			}
		}
		
		return StringUtils.replacePattern(sql, "\\s+", " ");
	}
	
	public PreparedStatement getStatement(Connection conn) throws SQLException {
		PreparedStatement prep = conn.prepareStatement(this.getSql());
		
		for(int i = 0; i<this.getWhereClauses().size(); i++) {
			WhereClause wc = this.getWhereClauses().get(i);
			
			if (wc.getOperator() != ProtoDBSearchOperator.In)
				prep.setObject(i + 1, wc.getValue());
			
		}
		
		return prep;
	}
	
	public void filterJoins() {
		// get a list of actual aliases used in the select
		List<String> columnAliasesIncluded = getAliasIncluded();
		
		// Add list of where clauses included
		columnAliasesIncluded.addAll(getWhereAliasIncluded());
		
		// get a list of aliases needed (include all joins in path if there is a hierarchy)
		for (JoinRow r : this.getJoinClause()) {
			setIncluded(columnAliasesIncluded, this.getJoinClause(), r, 0);
		}
	}
	
	private List<String> getWhereAliasIncluded() {
		List<String> result = new ArrayList<String>();
		for (WhereClause wc : this.getWhereClauses()) {
			String alias = wc.getSearchField().getAlias();
			if (!result.contains(alias))
				result.add(alias);
		}
		
		return result;
	}
	
	private List<String> getAliasIncluded() {
		List<String> result = new ArrayList<String>();
		for (Column c : this.getColumnResult().getColumns()) {
			String alias = c.getOtherAlias();
			if (!result.contains(alias))
				result.add(alias);
		}
		

		
		return result;
	}
	
	private void setIncluded(List<String> aliasesIncluded, List<JoinRow> allRows, JoinRow r, int level) {
		if (aliasesIncluded.contains(r.getAlias()) || level > 0) {
			r.setIncluded(true);	
			
			List<JoinRow> parents = getParentRow(allRows, r);
			for (JoinRow p : parents) {
				if (p != null)
					setIncluded(aliasesIncluded, allRows, p, level + 1);
			}
		}
		
	}
	

	private List<JoinRow> getParentRow(List<JoinRow> allRows, JoinRow r) {
		String aliasToFind = r.getParentAlias();
		List<JoinRow> rows = new ArrayList<JoinRow>();
		
		for (JoinRow current : allRows) {
			if (StringUtils.equalsIgnoreCase(aliasToFind, current.getAlias()))
				rows.add(current);
		}
		
		return rows;
	}

	public <T extends Message> Map<Integer, List<T>> getResultLink(T instance, ResultSet rs, boolean getBlobs, List<String> excludedObjects) throws SQLException, ProtoDBParserException {
		Map<Integer, List<T>> map = new HashMap<Integer, List<T>>();
		
		while (rs.next()) {
			int parentID = rs.getInt("L0__thisID");
			
			if (!map.containsKey(parentID)) {
				map.put(parentID, new ArrayList<T>()); 
			}
			
			map.get(parentID).add(
				getResult(instance, 
						rs, 
						StringUtils.EMPTY, 
						getBlobs, 
						excludedObjects));
		}
		
		return map;
	}
		
	public <T extends Message> List<T> getResult(T instance, ResultSet rs, boolean getBlobs, List<String> excludedObjects) throws SQLException, ProtoDBParserException {
		List<T> result = new ArrayList<T>();
		while (rs.next()) {
			result.add(
				getResult(instance, 
						rs, 
						StringUtils.EMPTY, 
						getBlobs, excludedObjects));
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
		
		for (FieldDescriptor f : scanner.getBlobFields()) {
			if (getBlobs &&
					!Populator.isExcludedField(f.getName(), excludedObjects)) {
				
				String alias = this.getAliases().get(parentHierarchy);
				String columnName = String.format("%s_%s", alias, f.getName());
				
				byte[] byteData = rs.getBytes(columnName);
				
				Populator.populateField(b, f, byteData);
			}
			else if (f.isRequired()) {
				b.setField(f, ByteString.EMPTY);
			}
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
