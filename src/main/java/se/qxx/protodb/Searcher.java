package se.qxx.protodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;

import com.google.protobuf.DynamicMessage;

import se.qxx.protodb.exceptions.IDFieldNotFoundException;
import se.qxx.protodb.exceptions.ProtoDBParserException;
import se.qxx.protodb.model.CaseInsensitiveMap;
import se.qxx.protodb.model.Column;
import se.qxx.protodb.model.ColumnResult;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

public class Searcher {

	public static JoinResult getJoinQuery(ProtoDBScanner scanner, boolean getBlobs, boolean travelComplexLinks) {
		return getJoinQuery(scanner, getBlobs, travelComplexLinks, null, StringUtils.EMPTY, -1, -1, null);
	}

	public static JoinResult getJoinQuery(ProtoDBScanner scanner, boolean getBlobs, boolean travelComplexLinks,
			List<String> excludedObjects) {
		return getJoinQuery(scanner, getBlobs, travelComplexLinks, null, StringUtils.EMPTY, -1, -1, excludedObjects);
	}

	public static JoinResult getJoinQuery(ProtoDBScanner scanner, boolean getBlobs, boolean travelComplexLinks,
			int numberOfResults, int offset) {
		return getJoinQuery(scanner, getBlobs, travelComplexLinks, null, StringUtils.EMPTY, numberOfResults, offset,
				null);
	}

	public static JoinResult getJoinQuery(ProtoDBScanner scanner, boolean getBlobs, boolean travelComplexLinks,
			int numberOfResults, int offset, List<String> excludedObjects) {
		return getJoinQuery(scanner, getBlobs, travelComplexLinks, null, StringUtils.EMPTY, numberOfResults, offset,
				excludedObjects);
	}

	public static JoinResult getJoinQuery(ProtoDBScanner scanner, boolean getBlobs, boolean travelComplexLinks,
			ProtoDBScanner other, String linkFieldName, int numberOfResults, int offset, List<String> excludedObjects) {

		CaseInsensitiveMap aliases = new CaseInsensitiveMap();
		String currentAlias = "A";
		aliases.put(StringUtils.EMPTY, currentAlias);

		// get a list of all aliases that are used
		// need to get the where clause and the sort clause as well
		// getJoinClause needs to return a set of aliases<->rows for each join
		ColumnResult columns = new ColumnResult();
		List<JoinRow> joinClause = new ArrayList<JoinRow>();

		// add the rest standard joins
		columns.append(
			Searcher.getColumnListForJoin(scanner, aliases, currentAlias, StringUtils.EMPTY, getBlobs,
				travelComplexLinks, excludedObjects));

		setMainTable(scanner, other, linkFieldName, columns, joinClause);

		joinClause.addAll(Searcher.getJoinClause(null, scanner, StringUtils.EMPTY, aliases, new MutableInt(1),
				StringUtils.EMPTY, StringUtils.EMPTY, travelComplexLinks, getBlobs, excludedObjects));

		// If complex join set a distinct on the first object only
		// This to do a simple search query. The result needs to be picked up by
		// the get query.

		return new JoinResult(joinClause, columns, aliases, columns.hasComplexJoins(), numberOfResults, offset,
				scanner.getBackend());

	}

	private static void setMainTable(ProtoDBScanner scanner, ProtoDBScanner other, String linkFieldName,
			ColumnResult columns, List<JoinRow> joinClause) {
		if (other != null && !StringUtils.isEmpty(linkFieldName)) {

			// if a link object was specified then we need the link table as main table
			joinClause.add(new JoinRow("", "L0",
					String.format("FROM %s AS L0 ", other.getLinkTableName(scanner, linkFieldName))));

			joinClause.add(new JoinRow("", "A",
					String.format("LEFT JOIN %s AS A ON L0.%s_%s_ID%s = A.ID ", scanner.getObjectName(),
							scanner.getBackend().getStartBracket(), scanner.getObjectName().toLowerCase(),
							scanner.getBackend().getEndBracket())));

			Column c1 = new Column("L0", "L0", String.format("_%s_ID", other.getObjectName().toLowerCase()), "_thisID",
					scanner.getBackend());

			Column c2 = new Column("L0", "L0", String.format("_%s_ID", scanner.getObjectName().toLowerCase()), "_otherID",
					scanner.getBackend());

			columns.getColumnList().add(c1);
			columns.getColumnList().add(c2);
			
			columns.addDistinctColumns(c1);
			columns.addDistinctColumns(c2);
		} else {
			joinClause.add(new JoinRow("", "A", String.format("FROM %s AS A ", scanner.getObjectName())));
		}
	}

	private static List<JoinRow> getJoinClauseRepeated(ProtoDBScanner parentScanner, ProtoDBScanner scanner,
			String parentFieldName, CaseInsensitiveMap aliases, MutableInt linkTableIterator, String parentHierarchy,
			String fieldHierarchy) {

		List<JoinRow> joinClause = new ArrayList<JoinRow>();

		if (parentScanner != null) {
			// set parent to empty on linktable since we do not want to traverse to parent on this one
			joinClause.add(new JoinRow(
					StringUtils.EMPTY, 
					aliases.get(fieldHierarchy),
					String.format("LEFT JOIN %s AS L%s  ON L%s._%s_ID = %s.ID ",
							parentScanner.getLinkTableName(scanner, parentFieldName), 
							linkTableIterator,
							linkTableIterator, 
							parentScanner.getObjectName().toLowerCase(),
							aliases.get(parentHierarchy))));

			joinClause.add(new JoinRow(
					aliases.get(parentHierarchy), 
					aliases.get(fieldHierarchy),
					String.format("LEFT JOIN %s AS %s  ON L%s._%s_ID = %s.ID ", 
							scanner.getObjectName(),
							aliases.get(fieldHierarchy), 
							linkTableIterator, 
							scanner.getObjectName().toLowerCase(),
							aliases.get(fieldHierarchy))));

			linkTableIterator.increment();
		}

		return joinClause;
	}

	private static List<JoinRow> getJoinClauseEnum(String enumFieldName, CaseInsensitiveMap aliases,
			String parentHierarchy, String fieldHierarchy) {
		List<JoinRow> joinClause = new ArrayList<JoinRow>();

		joinClause.add(new JoinRow(aliases.get(parentHierarchy), aliases.get(fieldHierarchy),
				String.format("LEFT JOIN %s AS %s  ON %s._%s_ID = %s.ID ", StringUtils.capitalize(enumFieldName),
						aliases.get(fieldHierarchy), aliases.get(parentHierarchy), enumFieldName,
						aliases.get(fieldHierarchy))));

		return joinClause;
	}

	private static List<JoinRow> getJoinClauseBlob(String blobFieldName, CaseInsensitiveMap aliases,
			String parentHierarchy, String fieldHierarchy) {
		List<JoinRow> joinClause = new ArrayList<JoinRow>();

		joinClause.add(new JoinRow(aliases.get(parentHierarchy), aliases.get(fieldHierarchy),
				String.format("LEFT JOIN BlobData AS %s  ON %s._%s_ID = %s.ID ", aliases.get(fieldHierarchy),
						aliases.get(parentHierarchy), blobFieldName, aliases.get(fieldHierarchy))));

		return joinClause;
	}

	private static List<JoinRow> getJoinClauseSimple(ProtoDBScanner parentScanner, ProtoDBScanner scanner,
			String parentFieldName, CaseInsensitiveMap aliases, String parentHierarchy, String fieldHierarchy) {
		List<JoinRow> joinClause = new ArrayList<JoinRow>();

		if (parentScanner != null) {
			joinClause.add(new JoinRow(aliases.get(parentHierarchy), aliases.get(fieldHierarchy),
					String.format("LEFT JOIN %s AS %s  ON %s._%s_ID = %s.ID ", scanner.getObjectName(),
							aliases.get(fieldHierarchy), aliases.get(parentHierarchy), parentFieldName.toLowerCase(),
							aliases.get(fieldHierarchy))));
		}

		return joinClause;
	}

	private static List<JoinRow> getJoinClause(ProtoDBScanner parentScanner, ProtoDBScanner scanner,
			String parentFieldName, CaseInsensitiveMap aliases, MutableInt linkTableIterator, String parentHierarchy,
			String fieldHierarchy, boolean travelComplexLinks, boolean getBlobs, List<String> excludedObjects) {

		// excluded objects is on the form field.field.field
		// this should be the same as the hierarchy?
		List<JoinRow> joinClause = new ArrayList<JoinRow>();

		if (travelComplexLinks) {
			for (FieldDescriptor f : scanner.getRepeatedObjectFields()) {
				if (getBlobs && f.getJavaType() == JavaType.BYTE_STRING) {

				} else {
					DynamicMessage mg = DynamicMessage.getDefaultInstance(f.getMessageType());
					ProtoDBScanner other = new ProtoDBScanner(mg, scanner.getBackend());
					String hierarchy = String.format("%s.%s", fieldHierarchy, f.getName());

					joinClause.addAll(getJoinClauseRepeated(scanner, other, f.getName(), aliases, linkTableIterator,
							fieldHierarchy, hierarchy));
					joinClause.addAll(getJoinClause(scanner, other, f.getName(), aliases, linkTableIterator,
							fieldHierarchy, hierarchy, travelComplexLinks, getBlobs, excludedObjects));
				}
			}
		}

		for (FieldDescriptor f : scanner.getObjectFields()) {
			String hierarchy = String.format("%s.%s", fieldHierarchy, f.getName());

			if (f.getJavaType() == JavaType.MESSAGE) {
				DynamicMessage mg = DynamicMessage.getDefaultInstance(f.getMessageType());
				ProtoDBScanner other = new ProtoDBScanner(mg, scanner.getBackend());

				joinClause.addAll(getJoinClauseSimple(scanner, other, f.getName(), aliases, fieldHierarchy, hierarchy));
				joinClause.addAll(getJoinClause(scanner, other, f.getName(), aliases, linkTableIterator, fieldHierarchy,
						hierarchy, travelComplexLinks, getBlobs, excludedObjects));
			} else if (f.getJavaType() == JavaType.ENUM) {
				joinClause.addAll(getJoinClauseEnum(f.getName(), aliases, fieldHierarchy, hierarchy));
			}
		}

		if (getBlobs) {
			for (FieldDescriptor f : scanner.getBlobFields()) {
				if (!Populator.isExcludedField(f.getName(), excludedObjects)) {
					String hierarchy = String.format("%s.%s", fieldHierarchy, f.getName());
					joinClause.addAll(getJoinClauseBlob(f.getName(), aliases, fieldHierarchy, hierarchy));
				}
			}
		}

		return joinClause;
	}

	public static ColumnResult getColumnListForJoin(ProtoDBScanner scanner, CaseInsensitiveMap aliases,
			String currentAlias, String parentHierarchy, boolean getBlobs, boolean travelComplexLinks,
			List<String> excludedObjects) {
		// the purpose of this is to create a sql query that joins all table together
		// Each column returned should have a prefix with the object identity followed
		// by
		// underscore and the column name. I.e. Object_field. This to avoid conflict
		// with
		// each other on field names. All link tables and foreign key columns should be
		// excluded.

		ColumnResult result = new ColumnResult();

		// set that the query result has complex joins that needs to be retreived
		// separately
		// do this ONLY if this is not a shallow search (i.e. a search that is supposed
		// not to travel the complex links)
		result.setHasComplexJoins(travelComplexLinks
				&& (scanner.getRepeatedObjectFields().size() > 0 || scanner.getRepeatedBasicFields().size() > 0));

		for (FieldDescriptor b : scanner.getBasicFields()) {
			result.append(currentAlias, b.getName(), scanner.getBackend());
		}

		int ac = 0;

		for (FieldDescriptor f : scanner.getObjectFields()) {
			String otherAlias = currentAlias + ((char) (65 + ac));
			String hierarchy = String.format("%s.%s", parentHierarchy, f.getName());
			aliases.put(hierarchy, otherAlias);

			if (f.getJavaType() == JavaType.MESSAGE) {
				DynamicMessage mg = DynamicMessage.getDefaultInstance(f.getMessageType());

				ProtoDBScanner other = new ProtoDBScanner(mg, scanner.getBackend());

				// must recurse even if excluded
				ColumnResult columnList = Searcher.getColumnListForJoin(other, aliases, otherAlias, hierarchy, getBlobs,
						travelComplexLinks, Populator.stripExcludedFields(f.getName(), excludedObjects));

				if (!Populator.isExcludedField(f.getName(), excludedObjects)) {
					result.append(columnList);
				}
			} else if (f.getJavaType() == JavaType.ENUM) {
				// Adding default value column for enum type
				result.append(currentAlias, otherAlias, "value", f.getName(), scanner.getBackend());
			}

			ac++;
		}

		if (getBlobs) {
			// TODO!
			// Add hierarchy and join to Blob table. Return the blob data as a column value
			for (FieldDescriptor f : scanner.getBlobFields()) {
				String otherAlias = currentAlias + ((char) (65 + ac));
				String hierarchy = String.format("%s.%s", parentHierarchy, f.getName());
				aliases.put(hierarchy, otherAlias);

				if (!Populator.isExcludedField(f.getName(), excludedObjects)) {
					result.append(currentAlias, otherAlias, "data", f.getName(), scanner.getBackend());
				}
				ac++;
			}
		}

		// set the distinct column list if this is the first object
		if (currentAlias == "A")
			result.setDistinctColumnList();

		if (travelComplexLinks) {
			for (FieldDescriptor f : scanner.getRepeatedObjectFields()) {
				String otherAlias = currentAlias + ((char) (65 + ac));

				DynamicMessage mg = DynamicMessage.getDefaultInstance(f.getMessageType());

				ProtoDBScanner other = new ProtoDBScanner(mg, scanner.getBackend());
				String hierarchy = String.format("%s.%s", parentHierarchy, f.getName());
				aliases.put(hierarchy, otherAlias);

				// parentHierarchy, fieldname
				ColumnResult columnList = Searcher.getColumnListForJoin(other, aliases, otherAlias, hierarchy, getBlobs,
						travelComplexLinks, Populator.stripExcludedFields(f.getName(), excludedObjects));

				if (!Populator.isExcludedField(f.getName(), excludedObjects)) {
					result.append(columnList);
				}

				ac++;
			}

			for (FieldDescriptor f : scanner.getRepeatedBasicFields()) {
				String otherAlias = currentAlias + ((char) (65 + ac));
				String hierarchy = String.format("%s.%s", parentHierarchy, f.getName());
				aliases.put(hierarchy, otherAlias);

				result.append(currentAlias, otherAlias, "value", f.getName(), scanner.getBackend());

				ac++;

			}
		}

		return result;
	}
}
