package femtodbexceptions;

public class FemtoDBByteArrayExceedsColumnSizeException extends FemtoDBException {
	private static final long serialVersionUID = 1L;

	public FemtoDBByteArrayExceedsColumnSizeException() {
		super(FemtoDBException.BYTE_ARRAY_EXCEEDS_COLUMN_SIZE);
	}


}
