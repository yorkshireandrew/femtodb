package femtodbexceptions;

public class AlteringOperationalTableException extends FemtoDBException {
	private static final long serialVersionUID = 1L;

	public AlteringOperationalTableException() {
		super(FemtoDBException.ALTERING_OPERATIONAL_TABLE);
	}


}
