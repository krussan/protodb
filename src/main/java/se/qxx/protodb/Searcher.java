package se.qxx.protodb;

import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;

import com.google.protobuf.DynamicMessage;

import se.qxx.protodb.model.ColumnResult;

import com.google.protobuf.Descriptors.FieldDescriptor;

public class Searcher {
	
	public static JoinResult getJoinQuery(ProtoDBScanner scanner, boolean getBlobs, boolean travelComplexLinks) {
		return getJoinQuery(scanner, getBlobs, travelComplexLinks, null, StringUtils.EMPTY, -1, -1);
	}
	
	public static JoinResult getJoinQuery(ProtoDBScanner scanner, boolean getBlobs, boolean travelComplexLinks, int numberOfResults, int offset) {
		return getJoinQuery(scanner, getBlobs, travelComplexLinks, null, StringUtils.EMPTY, numberOfResults, offset);
	}
	
	
	public static JoinResult getJoinQuery(ProtoDBScanner scanner, boolean getBlobs, boolean travelComplexLinks, ProtoDBScanner other, String linkFieldName, int numberOfResults, int offset) {
		HashMap<String, String> aliases = new HashMap<String, String>();
		String currentAlias = "A";
		aliases.put(StringUtils.EMPTY, "A");

		String linkTableJoin = StringUtils.EMPTY;
		String linkTableColumns = StringUtils.EMPTY;
		
		if (other != null && !StringUtils.isEmpty(linkFieldName)) {
			// if a link object was specified then we need the link table
			linkTableColumns = "L0._" + other.getObjectName().toLowerCase() + "_ID AS __thisID, "
					+  " L0._" + scanner.getObjectName().toLowerCase() + "_ID AS __otherID ";
			
			
			linkTableJoin = other.getLinkTableName(scanner, linkFieldName) + " L0";
		}
		
		ColumnResult columns = Searcher.getColumnListForJoin(scanner, aliases, currentAlias, StringUtils.EMPTY, getBlobs, travelComplexLinks);
		
		String joinList = Searcher.getJoinClause(null, scanner, StringUtils.EMPTY, aliases, new MutableInt(1), StringUtils.EMPTY, StringUtils.EMPTY, travelComplexLinks);
		
		// If complex join set a distinct on the first object only
		// This to do a simple search query. The result needs to be picked up by
		// the get query.
		String sql = String.format("SELECT %s%s%s FROM %s %s %s %s %s"
				, columns.hasComplexJoins() ? "DISTINCT " : ""
			    , StringUtils.isEmpty(linkTableColumns) ? "" : linkTableColumns + ", "
				, columns.hasComplexJoins() ? columns.getDistinctColumnList() : columns.getColumnListFinal()
				, linkTableJoin
				, StringUtils.isEmpty(linkTableJoin) ? "" : "LEFT JOIN"
				, scanner.getObjectName() + " AS A "
				, StringUtils.isEmpty(linkTableJoin) ? "" : " ON L0._" + scanner.getObjectName().toLowerCase() + "_ID = A.ID"
				, joinList);
		
		if (numberOfResults > 0) {
			sql += String.format(" LIMIT %s ", numberOfResults);
			if (offset > 0) {
				sql += String.format("OFFSET %s", offset);
			}
		}
			
		return new JoinResult(sql, aliases, columns.hasComplexJoins());
		 
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
	
	private static String getJoinClause(ProtoDBScanner parentScanner, ProtoDBScanner scanner, String parentFieldName, HashMap<String, String> aliases, MutableInt linkTableIterator, String parentHierarchy, String fieldHierarchy, boolean travelComplexLinks) {
		String joinClause = StringUtils.EMPTY;

		if (travelComplexLinks) {
			for (FieldDescriptor f : scanner.getRepeatedObjectFields()) {
				DynamicMessage mg = DynamicMessage.getDefaultInstance(f.getMessageType());
				ProtoDBScanner other = new ProtoDBScanner(mg);
				String hierarchy = String.format("%s.%s", fieldHierarchy, f.getName());
				
				joinClause += getJoinClauseRepeated(scanner, other, f.getName(), aliases, linkTableIterator, fieldHierarchy, hierarchy);
				joinClause += getJoinClause(scanner, other, f.getName(), aliases, linkTableIterator, fieldHierarchy, hierarchy, travelComplexLinks);
			}
		}
		
		for (FieldDescriptor f : scanner.getObjectFields()) { 
			DynamicMessage mg = DynamicMessage.getDefaultInstance(f.getMessageType());
			ProtoDBScanner other = new ProtoDBScanner(mg);
			String hierarchy = String.format("%s.%s", fieldHierarchy, f.getName());
			
			joinClause += getJoinClauseSimple(scanner, other, f.getName(), aliases, fieldHierarchy, hierarchy);
			joinClause += getJoinClause(scanner, other, f.getName(), aliases, linkTableIterator, fieldHierarchy, hierarchy, travelComplexLinks);
		}

		return joinClause;
	}
	

	public static ColumnResult getColumnListForJoin(ProtoDBScanner scanner, HashMap<String, String> aliases, String currentAlias, String parentHierarchy, boolean getBlobs, boolean travelComplexLinks) {
		// the purpose of this is to create a sql query that joins all table together
		// Each column returned should have a prefix with the object identity followed by
		// underscore and the column name. I.e. Object_field. This to avoid conflict with
		// each other on field names. All link tables and foreign key columns should be excluded.
		
		ColumnResult result = new ColumnResult();
		
		// set that the query result has complex joins that needs to be retreived separately
		// do this ONLY if this is not a shallow search (i.e. a search that is supposed not to travel the complex links)
		result.setHasComplexJoins(scanner.getRepeatedObjectFields().size() > 0 && travelComplexLinks);
		
		for (FieldDescriptor b : scanner.getBasicFields()) {
			result.append(String.format("%s.[%s] AS %s_%s, ", currentAlias, b.getName(), currentAlias, b.getName())); 
		}
		
		int ac = 0;
		
		for (FieldDescriptor f : scanner.getObjectFields()) {
			String otherAlias = currentAlias + ((char)(65 + ac));
			
			DynamicMessage mg = DynamicMessage.getDefaultInstance(f.getMessageType());

			ProtoDBScanner other = new ProtoDBScanner(mg);
			String hierarchy = String.format("%s.%s", parentHierarchy, f.getName());
			aliases.put(hierarchy, otherAlias);

			result.append(Searcher.getColumnListForJoin(other, aliases, otherAlias, hierarchy, getBlobs, travelComplexLinks));
			
			ac++;
		}
		
		// set the distinct column list if this is the first object
		if (currentAlias == "A")
			result.setDistinctColumnList();
		

		if (travelComplexLinks) {
			for (FieldDescriptor f : scanner.getRepeatedObjectFields()) {
				String otherAlias = currentAlias + ((char)(65 + ac));
				
				DynamicMessage mg = DynamicMessage.getDefaultInstance(f.getMessageType());
	
				ProtoDBScanner other = new ProtoDBScanner(mg);
				String hierarchy = String.format("%s.%s", parentHierarchy, f.getName());
				aliases.put(hierarchy, otherAlias);
	
				// parentHierarchy, fieldname
				result.append(Searcher.getColumnListForJoin(other, aliases, otherAlias, hierarchy, getBlobs, travelComplexLinks));
				
				ac++;
			}		
		}
		
		if (getBlobs) {
			//TODO!
			//Add hierarchy and join to Blob table. Return the blob data as a column value
			for (FieldDescriptor f : scanner.getBlobFields()) {
				String otherAlias = currentAlias + ((char)(65 + ac));

				String hierarchy = String.format("%s.%s", parentHierarchy, f.getName());
				aliases.put(hierarchy, otherAlias);

				result.append(String.format("%s.[data] AS %s_%s, ", otherAlias, otherAlias, f.getName()));
				ac++;	
			}
		}

		return result;
	}


}
