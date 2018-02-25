package se.qxx.protodb.backend;

public class ColumnDefinition {
	private String name;
	private String type;
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	public ColumnDefinition(String name, String type) {
		this.setName(name);
		this.setType(type);
	}
}
