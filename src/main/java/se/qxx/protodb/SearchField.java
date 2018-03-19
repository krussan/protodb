package se.qxx.protodb;

public class SearchField {
	private String fieldName;
	private String alias;
	public String getFieldName() {
		return fieldName;
	}
	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}
	public String getAlias() {
		return alias;
	}
	public void setAlias(String alias) {
		this.alias = alias;
	}
	
	public SearchField(String fieldName, String alias) {
		this.setFieldName(fieldName);
		this.setAlias(alias);
	}
	
	public String getAliasFieldName() {
		return String.format("%s.%s", this.getAlias(), this.getFieldName());
	}
}
