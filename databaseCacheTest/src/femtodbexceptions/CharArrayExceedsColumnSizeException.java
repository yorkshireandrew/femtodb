package femtodbexceptions;

public class CharArrayExceedsColumnSizeException extends FemtoDBException {
	private static final long serialVersionUID = 1L;

	public CharArrayExceedsColumnSizeException() {
		super(FemtoDBException.CHAR_ARRAY_EXCEEDS_COLUMN_SIZE);
	}


}
