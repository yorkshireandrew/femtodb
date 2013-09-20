package femtodbexceptions;

public class FemtoDBShuttingDownException extends FemtoDBException {
	private static final long serialVersionUID = 1L;

	public FemtoDBShuttingDownException() {
		super(FemtoDBException.SHUTTING_DOWN);
	}
	
	public FemtoDBShuttingDownException(String s) {
		super(FemtoDBException.SHUTTING_DOWN,s);
	}
	
	public FemtoDBShuttingDownException(String message,Throwable cause) {
		super(FemtoDBException.SHUTTING_DOWN,message,cause);
	}



}
