package se.qxx.protodb.exceptions;

public class ProtoDBParserException  extends Exception {

	public ProtoDBParserException(String objectType, Exception inner) {
		super(String.format("Parser error on protodb object :: %s. Error was :: %s", objectType, inner.getClass().getName()), inner);
	}
}
