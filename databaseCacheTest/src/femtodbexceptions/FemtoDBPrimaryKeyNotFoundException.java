package femtodbexceptions;

public class FemtoDBPrimaryKeyNotFoundException extends FemtoDBException {
	private static final long serialVersionUID = 1L;

	public FemtoDBPrimaryKeyNotFoundException() {
		super(FemtoDBException.PRIMARY_KEY_NOT_FOUND);
	}
	
	public FemtoDBPrimaryKeyNotFoundException(String s) {
		super(FemtoDBException.PRIMARY_KEY_NOT_FOUND,s);
	}
	
	public FemtoDBPrimaryKeyNotFoundException(String message,Throwable cause) {
		super(FemtoDBException.PRIMARY_KEY_NOT_FOUND,message,cause);
	}



}
