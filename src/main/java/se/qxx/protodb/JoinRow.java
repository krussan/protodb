package se.qxx.protodb;

public class JoinRow {
	private String alias;
	private String joinClause;
	private String parentAlias;
	private boolean isIncluded = false;
	
	public boolean isIncluded() {
		return isIncluded;
	}

	public void setIncluded(boolean isIncluded) {
		this.isIncluded = isIncluded;
	}

	public String getParentAlias() {
		return parentAlias;
	}

	public void setParentAlias(String parentAlias) {
		this.parentAlias = parentAlias;
	}

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public String getJoinCluase() {
		return joinClause;
	}

	public void setJoinCluase(String joinClause) {
		this.joinClause = joinClause;
	}

	public JoinRow(String parentAlias, String alias, String joinClause) {
		this.parentAlias = parentAlias;
		this.alias = alias;
		this.joinClause = joinClause;
	}
}
