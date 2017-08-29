package se.qxx.protodb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;

public class JoinResult {

	private HashMap<String, String> aliases = new HashMap<String,String>();
	private String joinClause = StringUtils.EMPTY;
	private List<String> whereClauses = new ArrayList<String>();
	private List<Object> whereParameters = new ArrayList<Object>();
	
	public JoinResult(String joinClause, HashMap<String, String> aliases) {
		this.setAliases(aliases);
		this.setJoinClause(joinClause);
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

	private void setWhereClauses(List<String> whereClauses) {
		this.whereClauses = whereClauses;
	}

	private List<Object> getWhereParameters() {
		return whereParameters;
	}

	private void setWhereParameters(List<Object> whereParameters) {
		this.whereParameters = whereParameters;
	}

	public void addWhereClause(String searchField, Object value, boolean isLikeOperator) {
		String key = "." + StringUtils.substringBeforeLast(searchField, ".");
		String field = StringUtils.substringAfterLast(searchField, ".");
		
		this.getWhereClauses().add(String.format("%s.%s %s ?", 
				this.getAliases().get(key),
				field,
				(isLikeOperator ? "LIKE" : "=")));
		
		this.getWhereParameters().add(value);
	}
	
	public String getSql() {
		return String.format("%s %s",this.getJoinClause(), this.getWhereClause());
	}
	
	public PreparedStatement getStatement(Connection conn) throws SQLException {
		PreparedStatement prep = conn.prepareStatement(this.getSql());
		
		for(int i = 0; i<this.getWhereParameters().size(); i++) {
			Object o = this.getWhereParameters().get(i);
			prep.setObject(i + 1, o);
		}
		
		return prep;
	}
	
	public <T extends Message> List<T> getResult(T instance, ResultSet rs) throws SQLException {
		List<T> result = new ArrayList<T>();
		while (rs.next()) {
			result.add(getResult(instance, rs, StringUtils.EMPTY));
		}
		
		return result;
	}

	@SuppressWarnings("unchecked")
	private <T extends Message> T getResult(T instance, ResultSet rs, String parentHierarchy) throws SQLException {
		Builder b = instance.newBuilderForType();

//		ProtoDBScanner scanner = new ProtoDBScanner(instance);
//		Log(String.format("Populating object %s :: %s", scanner.getObjectName(), id));
//		
//		// populate list of sub objects
//		populateRepeatedObjectFields(id, excludedObjects, conn, b, scanner);
//
//		// populate list of basic types
//		populateRepeatedBasicFields(id, conn, b, scanner);
		
		ProtoDBScanner scanner = new ProtoDBScanner(instance);
		
		for (FieldDescriptor f : scanner.getBasicFields()) {
			String alias = this.getAliases().get(parentHierarchy);
			String columnName = String.format("%s_%s", alias, f.getName());
			
			Object o = rs.getObject(columnName);
			Populator.populateField(b, f, o);
		}
		
		int ac = 0;
		
		for (FieldDescriptor f : scanner.getObjectFields()) {
			DynamicMessage mg = DynamicMessage.getDefaultInstance(f.getMessageType());
			String hierarchy = String.format("%s.%s", parentHierarchy, f.getName());
			
			mg = getResult(mg, rs, hierarchy);
			
			b.setField(f, mg);
			ac++;
		}
	
		return (T) b.build();
	}
	
	


}
