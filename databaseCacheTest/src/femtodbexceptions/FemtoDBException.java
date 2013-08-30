package femtodbexceptions;

public class FemtoDBException extends Exception {
	private static final long serialVersionUID = 1L;
	
	public static final int CHAR_ARRAY_EXCEEDS_COLUMN_SIZE 		= 1;
	public static final int STRING_EXCEEDS_COLUMN_SIZE 			= 2;
	public static final int ADDED_COLUMN_TO_OPERATIONAL_TABLE 	= 3;
	
	int subType = 0;
	FemtoDBException(int subtype){super();subType = subtype;}
	FemtoDBException(int subtype, String message){super(message);subType = subtype;}
	FemtoDBException(int subtype, String message, Throwable e){super(message,e);subType = subtype;}
	public int getSubType(){return subType;}
}
