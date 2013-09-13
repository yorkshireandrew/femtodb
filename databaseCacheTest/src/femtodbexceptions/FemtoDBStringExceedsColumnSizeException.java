package femtodbexceptions;

public class FemtoDBStringExceedsColumnSizeException extends FemtoDBException {
	private static final long serialVersionUID = 1L;

	public FemtoDBStringExceedsColumnSizeException() {
		super(FemtoDBException.STRING_EXCEEDS_COLUMN_SIZE);
	}


}
