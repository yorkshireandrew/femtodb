package femtodbexceptions;

public class FemtoBDCharArrayExceedsColumnSizeException extends FemtoDBException {
	private static final long serialVersionUID = 1L;

	public FemtoBDCharArrayExceedsColumnSizeException() {
		super(FemtoDBException.CHAR_ARRAY_EXCEEDS_COLUMN_SIZE);
	}


}
