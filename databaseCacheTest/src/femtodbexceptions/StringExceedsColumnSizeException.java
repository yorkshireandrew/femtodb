package femtodbexceptions;

public class StringExceedsColumnSizeException extends FemtoDBException {
	private static final long serialVersionUID = 1L;

	public StringExceedsColumnSizeException() {
		super(FemtoDBException.STRING_EXCEEDS_COLUMN_SIZE);
	}


}
