package se.qxx.protodb.exceptions;


public class IDFieldNotFoundException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7978543468488301743L;
	
	public IDFieldNotFoundException(String objectName)  {
		super(String.format("ID field not found on object :: %s", objectName));
	}
}
