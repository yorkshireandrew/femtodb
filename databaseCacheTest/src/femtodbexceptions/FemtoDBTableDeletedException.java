package femtodbexceptions;

public class FemtoDBTableDeletedException extends FemtoDBException {
	private static final long serialVersionUID = 1L;

	public FemtoDBTableDeletedException() {
		super(FemtoDBException.TABLE_DELETED);
	}
	
	public FemtoDBTableDeletedException(String s) {
		super(FemtoDBException.TABLE_DELETED,s);
	}
	
	public FemtoDBTableDeletedException(String message,Throwable cause) {
		super(FemtoDBException.TABLE_DELETED,message,cause);
	}



}
