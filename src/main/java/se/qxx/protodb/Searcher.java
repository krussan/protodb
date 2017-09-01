package se.qxx.protodb;

import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Descriptors.FieldDescriptor;

public class Searcher {
	public static JoinResult getJoinQuery(ProtoDBScanner scanner, boolean getBlobs) {
		HashMap<String, String> aliases = new HashMap<String, String>();
		String currentAlias = "A";
		aliases.put(StringUtils.EMPTY, "A");

		String columnList = Searcher.getColumnListForJoin(scanner, aliases, currentAlias, StringUtils.EMPTY, getBlobs);
		columnList = StringUtils.left(columnList, columnList.length() - 2);
		String joinList = Searcher.getJoinClause(null, scanner, StringUtils.EMPTY, aliases, new MutableInt(1), StringUtils.EMPTY, StringUtils.EMPTY);
		
		String sql = String.format("SELECT %s FROM %s AS A %s"
				, columnList
				, scanner.getObjectName()
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

			columnList += Searcher.getColumnListForJoin(other, aliases, otherAlias, hierarchy, getBlobs);
			
			ac++;
		}
		
		for (FieldDescriptor f : scanner.getRepeatedObjectFields()) {
			String otherAlias = currentAlias + ((char)(65 + ac));
			
			DynamicMessage mg = DynamicMessage.getDefaultInstance(f.getMessageType());

			ProtoDBScanner other = new ProtoDBScanner(mg);
			String hierarchy = String.format("%s.%s", parentHierarchy, f.getName());
			aliases.put(hierarchy, otherAlias);

			columnList += Searcher.getColumnListForJoin(other, aliases, otherAlias, hierarchy, getBlobs);
			
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
