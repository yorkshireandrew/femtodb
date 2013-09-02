package femtodbexceptions;

public class RowOverwriteException extends FemtoDBException {
	private static final long serialVersionUID = 1L;

	public RowOverwriteException() {
		super(FemtoDBException.ROW_OVERWRITE);
	}


}
