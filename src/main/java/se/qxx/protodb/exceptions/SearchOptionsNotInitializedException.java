package se.qxx.protodb.exceptions;

public class SearchOptionsNotInitializedException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4562550972736550834L;

	public SearchOptionsNotInitializedException() {
		super("Instance must be specified");
	}
}
