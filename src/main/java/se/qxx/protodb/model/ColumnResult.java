package se.qxx.protodb.model;

import org.apache.commons.lang3.StringUtils;

public class ColumnResult {

	private String columnList = StringUtils.EMPTY;
	private boolean hasComplexJoins = false;
	private String distinctColumnList = StringUtils.EMPTY;
	
	public String getDistinctColumnList() {
		return StringUtils.left(distinctColumnList, distinctColumnList.length() - 2);
	}

	public void setDistinctColumnList() {
		this.distinctColumnList = columnList;
	}

	public String getColumnListFinal() {
		return StringUtils.left(columnList, columnList.length() - 2);
	}
	
	public String getColumnList() {
		return columnList;
	}
	public void setColumnList(String columnList) {
		this.columnList = columnList;
	}
	public boolean hasComplexJoins() {
		return hasComplexJoins;
	}
	public void setHasComplexJoins(boolean hasComplexJoins) {
		this.hasComplexJoins = hasComplexJoins;
	}
	
	public ColumnResult() {
	}
	
	public void append(String columnList) {
		this.setColumnList(this.getColumnList() + columnList);
	}
	
	public void append(ColumnResult subResult) {
		if (subResult.hasComplexJoins)
			this.setHasComplexJoins(true);
		
		this.append(subResult.columnList);
	}	
}
