package se.qxx.protodb.model;

import java.awt.RenderingHints.Key;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import se.qxx.protodb.backend.DatabaseBackend;

public class ColumnResult {

	private List<Column> columnList = new ArrayList<Column>();
	private boolean hasComplexJoins = false;
	private List<Column> distinctColumnList = new ArrayList<Column>();
	
//	public String getDistinctColumnList() {
//		return StringUtils.left(distinctColumnList, distinctColumnList.length() - 2);
//	}

	public List<Column> getDistinctColumnList() {
		return this.distinctColumnList;
	}

	public void setDistinctColumnList() {
		distinctColumnList.clear();
		distinctColumnList.addAll(this.getColumnList());	
	}

//	public String getColumnListFinal() {
//		return StringUtils.left(columnList, columnList.length() - 2);
//	}
	
	public List<Column> getColumnList() {
		return columnList;
	}

	public boolean hasComplexJoins() {
		return hasComplexJoins;
	}
	
	public void setHasComplexJoins(boolean hasComplexJoins) {
		this.hasComplexJoins = hasComplexJoins;
	}
	
	public ColumnResult() {
	}
	
	public void append(String alias, String columnName, DatabaseBackend backend) {
		this.getColumnList().add(new Column(alias, alias, columnName, columnName, backend));
	}
	
	public void append(String alias, String otherAlias, String columnName, String columnAlias, DatabaseBackend backend) {
		this.getColumnList().add(new Column(alias, otherAlias, columnName, columnAlias, backend));
	}
	
	public void append(ColumnResult subResult) {
		if (subResult.hasComplexJoins)
			this.setHasComplexJoins(true);
		
		this.getColumnList().addAll(subResult.getColumnList());
		this.getDistinctColumnList().addAll(subResult.getDistinctColumnList());
	}	
	
	private String getSql(List<Column> columns) {
		String result = StringUtils.EMPTY;
		for (Column c : columns) {
			result += c.toString() + ", ";
		}
		
		return StringUtils.left(result, result.length() - 2);
	}
	
	public List<Column> getColumns() {
		return this.hasComplexJoins() ? this.getDistinctColumnList() : this.getColumnList();
	}
	
	public String getSql() {
		return this.getSql(this.getColumns());
	}
}
