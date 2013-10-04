package femtodbexceptions;

public class FemtoDBException extends Exception {
	private static final long serialVersionUID = 1L;
	
	public static final int BYTE_ARRAY_EXCEEDS_COLUMN_SIZE 		= 1;
	public static final int CHAR_ARRAY_EXCEEDS_COLUMN_SIZE 		= 2;
	public static final int STRING_EXCEEDS_COLUMN_SIZE 			= 3;
	public static final int ALTERING_OPERATIONAL_TABLE 			= 4;
	public static final int INVALID_VALUE			 			= 5;
	public static final int PRIMARY_KEY_USED					= 6;
	public static final int ROW_READ_LOCK						= 7;
	public static final int ROW_WRITE_LOCK						= 8;
	public static final int IO_EXCEPTION						= 9;
	public static final int PRIMARY_KEY_NOT_FOUND				= 10;
	public static final int CONCURRENT_MODIFICATION				= 11;
	public static final int SHUTTING_DOWN						= 12;
	public static final int TABLE_DELETED						= 13;
	
	int subType = 0;
	FemtoDBException(int subtype){super();subType = subtype;}
	FemtoDBException(int subtype, String message){super(message);subType = subtype;}
	FemtoDBException(int subtype, String message, Throwable e){super(message,e);subType = subtype;}
	public int getSubType(){return subType;}
}
