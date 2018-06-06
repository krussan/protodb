package se.qxx.protodb;

public class JoinRow {
	private String alias;
	private String joinCluase;
	
	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public String getJoinCluase() {
		return joinCluase;
	}

	public void setJoinCluase(String joinCluase) {
		this.joinCluase = joinCluase;
	}

	public JoinRow(String alias, String joinClause) {
		this.alias = alias;
		this.joinCluase = joinClause;
	}
}
