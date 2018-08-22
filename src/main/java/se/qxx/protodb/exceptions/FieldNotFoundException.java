package se.qxx.protodb.exceptions;


public class FieldNotFoundException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7978543468488301743L;
	
	public FieldNotFoundException(String fieldName, String objectName)  {
		super(String.format("Search field not found on object :: %s :: %s", fieldName, objectName));
	}
}
