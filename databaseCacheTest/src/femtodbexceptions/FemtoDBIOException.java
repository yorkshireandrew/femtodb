package femtodbexceptions;

public class FemtoDBIOException extends FemtoDBException {
	private static final long serialVersionUID = 1L;

	public FemtoDBIOException() {
		super(FemtoDBException.IO_EXCEPTION);
	}
	
	public FemtoDBIOException(String s) {
		super(FemtoDBException.IO_EXCEPTION,s);
	}
	
	public FemtoDBIOException(String message,Throwable cause) {
		super(FemtoDBException.IO_EXCEPTION,message,cause);
	}



}
