package se.qxx.protodb;

import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;

import com.google.protobuf.DynamicMessage;

import se.qxx.protodb.exceptions.IDFieldNotFoundException;
import se.qxx.protodb.exceptions.ProtoDBParserException;
import se.qxx.protodb.model.ColumnResult;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

public class Searcher {
	
	public static JoinResult getJoinQuery(ProtoDBScanner scanner, boolean getBlobs, boolean travelComplexLinks) {
		return getJoinQuery(scanner, getBlobs, travelComplexLinks, null, StringUtils.EMPTY, -1, -1, null);
	}
	
	public static JoinResult getJoinQuery(ProtoDBScanner scanner, boolean getBlobs, boolean travelComplexLinks, List<String> excludedObjects) {
		return getJoinQuery(scanner, getBlobs, travelComplexLinks, null, StringUtils.EMPTY, -1, -1, excludedObjects);
	}
	
	public static JoinResult getJoinQuery(ProtoDBScanner scanner, boolean getBlobs, boolean travelComplexLinks, int numberOfResults, int offset) {
		return getJoinQuery(scanner, getBlobs, travelComplexLinks, null, StringUtils.EMPTY, numberOfResults, offset, null);
	}
	
	public static JoinResult getJoinQuery(ProtoDBScanner scanner, boolean getBlobs, boolean travelComplexLinks, int numberOfResults, int offset, List<String> excludedObjects) {
		return getJoinQuery(scanner, getBlobs, travelComplexLinks, null, StringUtils.EMPTY, numberOfResults, offset, excludedObjects);
	}
	
	public static JoinResult getJoinQuery(
			ProtoDBScanner scanner, 
			boolean getBlobs, 
			boolean travelComplexLinks, 
			ProtoDBScanner other, 
			String linkFieldName, 
			int numberOfResults, 
			int offset,
			List<String> excludedObjects) {
		
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
		
		ColumnResult columns = Searcher.getColumnListForJoin(
				scanner, 
				aliases, 
				currentAlias, 
				StringUtils.EMPTY, 
				getBlobs, 
				travelComplexLinks,
				excludedObjects);
		
		String joinList = Searcher.getJoinClause(
				null, 
				scanner, 
				StringUtils.EMPTY, 
				aliases, 
				new MutableInt(1), 
				StringUtils.EMPTY, 
				StringUtils.EMPTY, 
				travelComplexLinks, 
				getBlobs, 
				excludedObjects);
		
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
			
		return new JoinResult(sql, aliases, columns.hasComplexJoins(), numberOfResults, offset, scanner.getBackend());
		 
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
	
	private static String getJoinClauseRepeatedBasic(ProtoDBScanner parentScanner, FieldDescriptor parentField , HashMap<String, String> aliases, MutableInt linkTableIterator, String parentHierarchy, String fieldHierarchy) {
		return String.format("LEFT JOIN %s AS L%s ",
				parentScanner.getBasicLinkTableName(parentField),
				linkTableIterator);
	
	}

	private static String getJoinClauseRepeatedBlob(ProtoDBScanner parentScanner, ProtoDBScanner scanner, String parentFieldName, HashMap<String, String> aliases, MutableInt linkTableIterator, String parentHierarchy, String fieldHierarchy) {
		
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

	private static String getJoinClauseEnum(String enumFieldName, HashMap<String, String> aliases, String parentHierarchy, String fieldHierarchy) {
		String joinClause = StringUtils.EMPTY;
		
		joinClause += String.format("LEFT JOIN %s AS %s ", 
			StringUtils.capitalize(enumFieldName), 
			aliases.get(fieldHierarchy));
		
		joinClause += String.format(" ON %s._%s_ID = %s.ID ",
			aliases.get(parentHierarchy),
			enumFieldName,
			aliases.get(fieldHierarchy));
	
		
		return joinClause;
	}
	
	private static String getJoinClauseBlob(String blobFieldName, HashMap<String, String> aliases, String parentHierarchy, String fieldHierarchy) {
		String joinClause = StringUtils.EMPTY;
		
		joinClause += String.format("LEFT JOIN BlobData AS %s ",  
			aliases.get(fieldHierarchy));
		
		joinClause += String.format(" ON %s._%s_ID = %s.ID ",
			aliases.get(parentHierarchy),
			blobFieldName,
			aliases.get(fieldHierarchy));
	
		
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
	
	private static String getJoinClause(ProtoDBScanner parentScanner, ProtoDBScanner scanner, String parentFieldName, HashMap<String, String> aliases, MutableInt linkTableIterator, String parentHierarchy, String fieldHierarchy, boolean travelComplexLinks, boolean getBlobs, List<String> excludedObjects) {
		// excluded objects is on the form field.field.field
		// this should be the same as the hierarchy?
		
		String joinClause = StringUtils.EMPTY;

		if (travelComplexLinks) {
			for (FieldDescriptor f : scanner.getRepeatedObjectFields()) {
				if (getBlobs && f.getJavaType() == JavaType.BYTE_STRING) {
					
				}
				else {
					DynamicMessage mg = DynamicMessage.getDefaultInstance(f.getMessageType());
					ProtoDBScanner other = new ProtoDBScanner(mg, scanner.getBackend());
					String hierarchy = String.format("%s.%s", fieldHierarchy, f.getName());
					
					joinClause += getJoinClauseRepeated(scanner, other, f.getName(), aliases, linkTableIterator, fieldHierarchy, hierarchy);
					joinClause += getJoinClause(scanner, other, f.getName(), aliases, linkTableIterator, fieldHierarchy, hierarchy, travelComplexLinks, getBlobs, excludedObjects);
				}
			}
		}
		
		for (FieldDescriptor f : scanner.getObjectFields()) {
			String hierarchy = String.format("%s.%s", fieldHierarchy, f.getName());
			
			if (f.getJavaType() == JavaType.MESSAGE) {
				DynamicMessage mg = DynamicMessage.getDefaultInstance(f.getMessageType());
				ProtoDBScanner other = new ProtoDBScanner(mg, scanner.getBackend());
				
				joinClause += getJoinClauseSimple(scanner, other, f.getName(), aliases, fieldHierarchy, hierarchy);
				joinClause += getJoinClause(scanner, other, f.getName(), aliases, linkTableIterator, fieldHierarchy, hierarchy, travelComplexLinks, getBlobs, excludedObjects);
			}
			else if (f.getJavaType() == JavaType.ENUM) {
				joinClause += getJoinClauseEnum(f.getName(), aliases, fieldHierarchy, hierarchy);
			}
		}

		if (getBlobs) {
			for (FieldDescriptor f : scanner.getBlobFields()) {
				if (!Populator.isExcludedField(f.getName(), excludedObjects)) {
					String hierarchy = String.format("%s.%s", fieldHierarchy, f.getName());
					joinClause += getJoinClauseBlob(f.getName(), aliases, fieldHierarchy, hierarchy);
				}
			}
		}

		return joinClause;
	}
	

	public static ColumnResult getColumnListForJoin(
			ProtoDBScanner scanner, 
			HashMap<String, String> aliases, 
			String currentAlias, 
			String parentHierarchy, 
			boolean getBlobs, 
			boolean travelComplexLinks,
			List<String> excludedObjects) {
		// the purpose of this is to create a sql query that joins all table together
		// Each column returned should have a prefix with the object identity followed by
		// underscore and the column name. I.e. Object_field. This to avoid conflict with
		// each other on field names. All link tables and foreign key columns should be excluded.
		
		ColumnResult result = new ColumnResult();
		
		// set that the query result has complex joins that needs to be retreived separately
		// do this ONLY if this is not a shallow search (i.e. a search that is supposed not to travel the complex links)
		result.setHasComplexJoins(travelComplexLinks &&
				(scanner.getRepeatedObjectFields().size() > 0 || scanner.getRepeatedBasicFields().size() > 0) );
		
		for (FieldDescriptor b : scanner.getBasicFields()) {
			result.append(String.format("%s.%s%s%s AS %s_%s, ", 
				currentAlias,
				scanner.getBackend().getStartBracket(),
				b.getName(),
				scanner.getBackend().getEndBracket(),
				currentAlias, 
				b.getName())); 
		}
		
		int ac = 0;
		
		for (FieldDescriptor f : scanner.getObjectFields()) {
			String otherAlias = currentAlias + ((char)(65 + ac));
			String hierarchy = String.format("%s.%s", parentHierarchy, f.getName());
			aliases.put(hierarchy, otherAlias);

			if (f.getJavaType() == JavaType.MESSAGE) {
				DynamicMessage mg = DynamicMessage.getDefaultInstance(f.getMessageType());
	
				ProtoDBScanner other = new ProtoDBScanner(mg, scanner.getBackend());

				// must recurse even if excluded
				ColumnResult columnList = Searcher.getColumnListForJoin(
						other, 
						aliases, 
						otherAlias, 
						hierarchy, 
						getBlobs, 
						travelComplexLinks,
						Populator.stripExcludedFields(
								f.getName(), 
								excludedObjects));
					
				if (!Populator.isExcludedField(f.getName(), excludedObjects)) {
					result.append(columnList);
				}
			}
			else if (f.getJavaType() == JavaType.ENUM) {
				// Adding default value column for enum type
				result.append(String.format("%s.%svalue%s AS %s_%s, ", 
					otherAlias,
					scanner.getBackend().getStartBracket(),
					scanner.getBackend().getEndBracket(),
					currentAlias, 
					f.getName()));
			}
			
			ac++;			
		}

		if (getBlobs) {
			//TODO!
			//Add hierarchy and join to Blob table. Return the blob data as a column value
			for (FieldDescriptor f : scanner.getBlobFields()) {
				String otherAlias = currentAlias + ((char)(65 + ac));
				String hierarchy = String.format("%s.%s", parentHierarchy, f.getName());
				aliases.put(hierarchy, otherAlias);

				if (!Populator.isExcludedField(f.getName(), excludedObjects)) {

					result.append(String.format("%s.%sdata%s AS %s_%s, ", 
						otherAlias,
						scanner.getBackend().getStartBracket(),
						scanner.getBackend().getEndBracket(),
						currentAlias, 
						f.getName()));
						
				}
				ac++;
			}
		}

		// set the distinct column list if this is the first object
		if (currentAlias == "A")
			result.setDistinctColumnList();
		

		if (travelComplexLinks) {
			for (FieldDescriptor f : scanner.getRepeatedObjectFields()) {
				String otherAlias = currentAlias + ((char)(65 + ac));
			
				DynamicMessage mg = DynamicMessage.getDefaultInstance(f.getMessageType());
		
				ProtoDBScanner other = new ProtoDBScanner(mg, scanner.getBackend());
				String hierarchy = String.format("%s.%s", parentHierarchy, f.getName());
				aliases.put(hierarchy, otherAlias);

			
				// parentHierarchy, fieldname
				ColumnResult columnList =
						Searcher.getColumnListForJoin(
								other, 
								aliases, 
								otherAlias, 
								hierarchy, 
								getBlobs, 
								travelComplexLinks,
								Populator.stripExcludedFields(
										f.getName(), 
										excludedObjects));
										
				if (!Populator.isExcludedField(f.getName(), excludedObjects)) {
					result.append(columnList);
				}
		
				ac++;
			}
			
			for (FieldDescriptor f : scanner.getRepeatedBasicFields()) {
				String otherAlias = currentAlias + ((char)(65 + ac));
				String hierarchy = String.format("%s.%s", parentHierarchy, f.getName());
				aliases.put(hierarchy, otherAlias);

				result.append(String.format("%s.%svalue%s AS %s_%s, ", 
					otherAlias,
					scanner.getBackend().getStartBracket(),
					scanner.getBackend().getEndBracket(),
					currentAlias, 
					f.getName()));
				
				ac++;	
				
			}
		}
		

		return result;
	}
}
