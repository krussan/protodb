package se.qxx.protodb.model;

import se.qxx.protodb.backend.DatabaseBackend;

public class Column {

	private String alias;
	private String otherAlias;
	private String columnName;
	private String columnAlias;
	private DatabaseBackend backend;

	public String getOtherAlias() {
		return otherAlias;
	}

	public void setOtherAlias(String otherAlias) {
		this.otherAlias = otherAlias;
	}


	public String getColumnAlias() {
		return columnAlias;
	}

	public void setColumnAlias(String columnAlias) {
		this.columnAlias = columnAlias;
	}

	public String getAlias() {
		return alias;
	}

	public DatabaseBackend getBackend() {
		return backend;
	}

	public void setBackend(DatabaseBackend backend) {
		this.backend = backend;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public Column(String alias, String otherAlias, String columnName, String columnAlias, DatabaseBackend backend) {
		this.setAlias(alias);
		this.setOtherAlias(otherAlias);
		this.setColumnName(columnName);
		this.setColumnAlias(columnAlias);
		this.setBackend(backend);
	}
	
	@Override
	public String toString() {
		return String.format("%s.%s%s%s AS %s_%s, ", 
				this.getOtherAlias(),
				this.getBackend().getStartBracket(),
				this.getColumnName(),
				this.getBackend().getEndBracket(),
				this.getAlias(),
				this.getColumnAlias());
	}
}
