package femtodb;

import java.io.UTFDataFormatException;

/** Class that represents a row taken from, or to be inserted into a database tableCore */
public class RowAccessType {
	/** The RowAccessTypeFactory that created this object so it can be reclaimed */
	private RowAccessTypeFactory 	source;
			long 					primaryKey;
			short 					flags;
			TableCore 				tableCore;
			byte[] 					byteArray;
	private boolean 				flagsInvalid;
	
	RowAccessType(final long primaryKey, final short flags, final TableCore tableCore, final byte[] byteArray, final RowAccessTypeFactory source)
	{
		this.primaryKey 	= primaryKey;
		this.flags 			= flags;
		this.tableCore 		= tableCore;
		this.byteArray 		= byteArray;
		this.source 		= source;
		this.flagsInvalid 	= (flags == TableCore.FLAG_CACHE_NOT_SET);
	}
	
	public void close(){source.reclaim(this);}
	public long getPrimaryKey(){return primaryKey;}
	
	public boolean getFlag1()
	{
		ensureFlagsValid();
		return((flags|TableCore.ROW_WRITELOCK) != 0);
	}
	
	public boolean getFlag2()
	{
		ensureFlagsValid();
		return((flags|TableCore.ROW_READLOCK) != 0);
	}
	
	private void ensureFlagsValid()
	{
		if(flagsInvalid)
		{
			flags = BuffRead.readShort(byteArray, 8);
			flagsInvalid = false;
		}		
	}
	
	// **********************************************************
	// ************* GET METHODS FOR PRIMITIVE TYPES ************
	// **********************************************************

	/** Returns the byte in the given column, or -1 if null */	
	public final byte get_byte(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return -1;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return (byte)BuffRead.read(byteArray, columnByteOffset[column]);	
	}

	/** Returns the byte array in the given column, or null if null*/	
	public final byte[] get_byteArray(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return null;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return BuffRead.readByteArray(byteArray, columnByteOffset[column]);	
	}
	
	/** Returns the boolean value in the given column, or false if null */
	public final boolean get_boolean(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return false;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return BuffRead.readBoolean(byteArray, columnByteOffset[column]);	
	}
	
	/** Returns the short value in the given column, or -1 if null */
	public final short get_short(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return -1;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return BuffRead.readShort(byteArray, columnByteOffset[column]);	
	}
	
	/** Returns the char value in the given column, or -1 if null */
	public final char get_char(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return (char) -1;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return BuffRead.readChar(byteArray, columnByteOffset[column]);	
	}
	
	/** Returns the int value in the given column, or -1 if null */
	public final int get_int(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return -1;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return BuffRead.readInt(byteArray, columnByteOffset[column]);	
	}
	
	/** Returns the long value in the given column, or -1L if null */
	public final long get_long(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return -1L;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return BuffRead.readLong(byteArray, columnByteOffset[column]);	
	}	
	
	/** Returns the float value in the given column, or NaN if null */
	public final float get_float(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return Float.NaN;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return BuffRead.readFloat(byteArray, columnByteOffset[column]);	
	}
	
	/** Returns the double value in the given column, or NaN if null */
	public final double get_double(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return Double.NaN;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return BuffRead.readDouble(byteArray, columnByteOffset[column]);	
	}
	
	/** Returns the char array in the given column, or null if null */
	public final char[] get_charArray(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return null;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return BuffRead.readChars(byteArray, columnByteOffset[column]);	
	}
	
	// *****************************************************************
	// ************* GET METHODS FOR WRAPPER/OBJECT TYPES   ************
	// *****************************************************************

	/** Returns a Byte object in the given column, or null */
	public final Byte getByte(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return null;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return new Byte((byte)BuffRead.read(byteArray, columnByteOffset[column]));	
	}
	
	/** Returns a Boolean object in the given column, or null */
	public final Boolean getBoolean(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return null;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return new Boolean(BuffRead.readBoolean(byteArray, columnByteOffset[column]));	
	}

	/** Returns a Short object in the given column, or null */
	public final Short getShort(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return null;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return new Short(BuffRead.readShort(byteArray, columnByteOffset[column]));	
	}
	
	/** Returns a Character object in the given column, or null */
	public final Character getCharacter(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return null;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return new Character(BuffRead.readChar(byteArray, columnByteOffset[column]));	
	}

	/** Returns a Integer object in the given column, or null */	
	public final Integer getInteger(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return null;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return new Integer(BuffRead.readInt(byteArray, columnByteOffset[column]));	
	}	
	
	/** Returns a Long object in the given column, or null */	
	public final Long getLong(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return null;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return new Long(BuffRead.readLong(byteArray, columnByteOffset[column]));	
	}
	
	/** Returns a Float object in the given column, or null */	
	public final Float getFloat(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return null;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return new Float(BuffRead.readFloat(byteArray, columnByteOffset[column]));	
	}
	
	/** Returns a Double object in the given column, or null */	
	public final Double getDouble(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return null;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return new Double(BuffRead.readDouble(byteArray, columnByteOffset[column]));	
	}
	
	/** Returns the string in the given column, or null if null */
	public final String getString(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return null;
		int[] columnByteOffset = tableCore.columnByteOffset;
		try {
			return BuffRead.readString(byteArray, columnByteOffset[column]);
		} catch (UTFDataFormatException e) {
			return null;
		}	
	}	
	
	/** Returns a modified string builder using the data in the given column, or null if null */
	public final StringBuilder getString(final int column, StringBuilder sb)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return null;
		int[] columnByteOffset = tableCore.columnByteOffset;
		try {
			return BuffRead.readStringBuilder(byteArray, columnByteOffset[column],sb);
		} catch (UTFDataFormatException e) {
			return null;
		}	
	}
	
	
	
	
	
	
}
