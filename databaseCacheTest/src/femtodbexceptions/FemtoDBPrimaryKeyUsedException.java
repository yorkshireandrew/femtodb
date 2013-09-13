package femtodbexceptions;

public class FemtoDBPrimaryKeyUsedException extends FemtoDBException {
	private static final long serialVersionUID = 1L;

	public FemtoDBPrimaryKeyUsedException() {
		super(FemtoDBException.PRIMARY_KEY_USED);
	}
	
	public FemtoDBPrimaryKeyUsedException(String message) {
		super(FemtoDBException.PRIMARY_KEY_USED, message);
	}
}
