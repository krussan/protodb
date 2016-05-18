package se.qxx.protodb.exceptions;


public class SearchFieldNotFoundException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7978543468488301743L;
	
	public SearchFieldNotFoundException(String fieldName, String objectName)  {
		super(String.format("Search field not found on object :: %s :: %s", fieldName, objectName));
	}
}
