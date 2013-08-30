package femtodbexceptions;

public class InvalidValueException extends FemtoDBException {
	private static final long serialVersionUID = 1L;

	public InvalidValueException() {
		super(FemtoDBException.INVALID_VALUE);
	}
	
	public InvalidValueException(String s) {
		super(FemtoDBException.INVALID_VALUE,s);
	}


}
