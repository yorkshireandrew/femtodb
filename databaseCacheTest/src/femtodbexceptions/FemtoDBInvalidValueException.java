package femtodbexceptions;

public class FemtoDBInvalidValueException extends FemtoDBException {
	private static final long serialVersionUID = 1L;

	public FemtoDBInvalidValueException() {
		super(FemtoDBException.INVALID_VALUE);
	}
	
	public FemtoDBInvalidValueException(String s) {
		super(FemtoDBException.INVALID_VALUE,s);
	}


}
