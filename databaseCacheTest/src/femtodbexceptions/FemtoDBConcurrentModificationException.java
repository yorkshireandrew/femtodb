package femtodbexceptions;

public class FemtoDBConcurrentModificationException extends FemtoDBException {
	private static final long serialVersionUID = 1L;

	public FemtoDBConcurrentModificationException() {
		super(FemtoDBException.CONCURRENT_MODIFICATION);
	}
	
	public FemtoDBConcurrentModificationException(String s) {
		super(FemtoDBException.CONCURRENT_MODIFICATION,s);
	}
	
	public FemtoDBConcurrentModificationException(String message,Throwable cause) {
		super(FemtoDBException.CONCURRENT_MODIFICATION,message,cause);
	}



}
