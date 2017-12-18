package se.qxx.protodb.model;

public class SearchArgument {
	private String value;
	private boolean isLikeFilter;
	
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public boolean isLikeFilter() {
		return isLikeFilter;
	}
	public void setLikeFilter(boolean isLikeFilter) {
		this.isLikeFilter = isLikeFilter;
	}
}
