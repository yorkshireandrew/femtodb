package femtodb;

import java.io.UTFDataFormatException;

import femtodbexceptions.FemtoDBByteArrayExceedsColumnSizeException;
import femtodbexceptions.FemtoBDCharArrayExceedsColumnSizeException;
import femtodbexceptions.FemtoDBStringExceedsColumnSizeException;

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
	
	/** Called by insert or update just prior to persisting the RowAccessType to the database.
	 *  It copies the flags field for the RowAccessType flags field into the cache. This is a package scope method.
	 */
	void prepareFlagsForPersisting()
	{
		if(!flagsInvalid)
		{
			BuffWrite.writeShort(byteArray, 8, flags);
		}		
	}
	
	// **********************************************************
	// ************* GET METHODS FOR PRIMITIVE TYPES ************
	// **********************************************************

	/** Returns the byte primitive type in the given column, or -1 if null */	
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
	
	/** Returns the boolean primitive type in the given column, or false if null */
	public final boolean get_boolean(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return false;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return BuffRead.readBoolean(byteArray, columnByteOffset[column]);	
	}
	
	/** Returns the short primitive type in the given column, or -1 if null */
	public final short get_short(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return -1;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return BuffRead.readShort(byteArray, columnByteOffset[column]);	
	}
	
	/** Returns the char primitive type in the given column, or -1 if null */
	public final char get_char(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return (char) -1;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return BuffRead.readChar(byteArray, columnByteOffset[column]);	
	}
	
	/** Returns the int primitive type in the given column, or -1 if null */
	public final int get_int(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return -1;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return BuffRead.readInt(byteArray, columnByteOffset[column]);	
	}
	
	/** Returns the long primitive type in the given column, or -1L if null */
	public final long get_long(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return -1L;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return BuffRead.readLong(byteArray, columnByteOffset[column]);	
	}	
	
	/** Returns the float primitive type in the given column, or NaN if null */
	public final float get_float(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return Float.NaN;
		int[] columnByteOffset = tableCore.columnByteOffset;
		return BuffRead.readFloat(byteArray, columnByteOffset[column]);	
	}
	
	/** Returns the double primitive type in the given column, or NaN if null */
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
		return BuffRead.readCharArray(byteArray, columnByteOffset[column]);	
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
	
	/** Returns true if the given column contains a null value */
	public final boolean isColumnNull(final int column)
	{
		ensureFlagsValid();
		if((flags|(1 << column)) == 0)return true;
		return false;
		
	}
	
	// **********************************************************
	// ************* SET METHODS FOR WRAPPER/OBJECT TYPES ************
	// **********************************************************
	
	/** Inserts a Byte object or null into the given column */
	public final void setByte(final int column, final Byte val)
	{
		if(val == null)
		{
			flags &= ~(1 << column);
		}
		else
		{
			flags |= (1 << column);
			int[] columnByteOffset = tableCore.columnByteOffset;
			BuffWrite.write(byteArray, columnByteOffset[column],val);
		}
	}
	
	/** Inserts a Boolean object or null into the given column */
	public final void setBoolean(final int column, final Boolean val)
	{
		if(val == null)
		{
			flags &= ~(1 << column);
		}
		else
		{
			flags |= (1 << column);
			int[] columnByteOffset = tableCore.columnByteOffset;
			BuffWrite.writeBoolean(byteArray, columnByteOffset[column],val);
		}
	}
	
	/** Inserts a Short object or null  into the given column */
	public final void setShort(final int column, final Short val)
	{
		if(val == null)
		{
			flags &= ~(1 << column);
		}
		else
		{
			flags |= (1 << column);
			int[] columnByteOffset = tableCore.columnByteOffset;
			BuffWrite.writeShort(byteArray, columnByteOffset[column],val);
		}
	}
	
	/** Inserts a Character object or null into the given column */
	public final void setCharacter(final int column, final Character val)
	{
		if(val == null)
		{
			flags &= ~(1 << column);
		}
		else
		{
			flags |= (1 << column);
			int[] columnByteOffset = tableCore.columnByteOffset;
			BuffWrite.writeChar(byteArray, columnByteOffset[column],val);
		}
	}
	
	/** Inserts a Integer object or null into the given column */
	public final void setInteger(final int column, final Integer val)
	{
		if(val == null)
		{
			flags &= ~(1 << column);
		}
		else
		{
			flags |= (1 << column);
			int[] columnByteOffset = tableCore.columnByteOffset;
			BuffWrite.writeInt(byteArray, columnByteOffset[column],val);
		}
	}

	/** Inserts a Long object or null into the given column */
	public final void setLong(final int column, final Long val)
	{
		if(val == null)
		{
			flags &= ~(1 << column);
		}
		else
		{
			flags |= (1 << column);
			int[] columnByteOffset = tableCore.columnByteOffset;
			BuffWrite.writeLong(byteArray, columnByteOffset[column],val);
		}
	}
	
	/** Inserts a Float object or null into the given column */
	public final void setFloat(final int column, final Float val)
	{
		if(val == null)
		{
			flags &= ~(1 << column);
		}
		else
		{
			flags |= (1 << column);
			int[] columnByteOffset = tableCore.columnByteOffset;
			BuffWrite.writeFloat(byteArray, columnByteOffset[column],val);
		}
	}
	
	/** Inserts a Double object or null into the given column */
	public final void setDouble(final int column, final Double val)
	{
		if(val == null)
		{
			flags &= ~(1 << column);
		}
		else
		{
			flags |= (1 << column);
			int[] columnByteOffset = tableCore.columnByteOffset;
			BuffWrite.writeDouble(byteArray, columnByteOffset[column],val);
		}
	}
	
	/** Inserts a String object or null into the given column. Throws a FemtoDBStringExceedsColumnSizeException if it will not fit */
	public final void setString(final int column, final String val) throws FemtoDBStringExceedsColumnSizeException
	{
		if(val == null)
		{
			flags &= ~(1 << column);
		}
		else
		{
			int[] columnByteOffset 	= tableCore.columnByteOffset;
			int[] columnByteWidth 	= tableCore.columnByteWidth;
			int columnByteWidth2 = columnByteWidth[column];
			BuffWrite.writeString(byteArray, columnByteOffset[column], columnByteWidth2, val);
			flags |= (1 << column);
		}
	}
	
	// **********************************************************
	// ************* SET METHODS FOR PRIMATIVE TYPES ************
	// **********************************************************

	/** Inserts a byte primitive type into the given column */
	public final void set_byte(final int column, final byte val)
	{
		flags |= (1 << column);
		int[] columnByteOffset = tableCore.columnByteOffset;
		BuffWrite.write(byteArray, columnByteOffset[column],val);
	}
	
	/** Inserts a byte array into the given column. Throws a FemtoDBByteArrayExceedsColumnSizeException if it will not fit */
	public final void set_byteArray(final int column, final byte[] val) throws FemtoDBByteArrayExceedsColumnSizeException
	{
		int[] columnByteOffset 	= tableCore.columnByteOffset;
		int[] columnByteWidth 	= tableCore.columnByteWidth;
		int columnByteWidth2 = columnByteWidth[column];
		if((4 + val.length) > columnByteWidth2) throw new FemtoDBByteArrayExceedsColumnSizeException();
		flags |= (1 << column);
		BuffWrite.writeByteArray(byteArray, columnByteOffset[column], val);
	}
	
	/** Inserts a boolean primitive type into the given column */
	public final void set_boolean(final int column, final boolean val)
	{
		flags |= (1 << column);
		int[] columnByteOffset = tableCore.columnByteOffset;
		BuffWrite.writeBoolean(byteArray, columnByteOffset[column],val);
	}
	
	/** Inserts a short primitive type into the given column */
	public final void set_short(final int column, final short val)
	{
		flags |= (1 << column);
		int[] columnByteOffset = tableCore.columnByteOffset;
		BuffWrite.writeShort(byteArray, columnByteOffset[column],val);
	}	
	
	/** Inserts a char primitive type into the given column */
	public final void set_char(final int column, final char val)
	{
		flags |= (1 << column);
		int[] columnByteOffset = tableCore.columnByteOffset;
		BuffWrite.writeChar(byteArray, columnByteOffset[column],val);
	}
	
	/** Inserts a int primitive type into the given column */
	public final void set_int(final int column, final int val)
	{
		flags |= (1 << column);
		int[] columnByteOffset = tableCore.columnByteOffset;
		BuffWrite.writeInt(byteArray, columnByteOffset[column],val);
	}

	/** Inserts a long primitive type into the given column */
	public final void set_long(final int column, final long val)
	{
		flags |= (1 << column);
		int[] columnByteOffset = tableCore.columnByteOffset;
		BuffWrite.writeLong(byteArray, columnByteOffset[column],val);
	}
	
	/** Inserts a float primitive type into the given column */
	public final void set_float(final int column, final float val)
	{
		flags |= (1 << column);
		int[] columnByteOffset = tableCore.columnByteOffset;
		BuffWrite.writeFloat(byteArray, columnByteOffset[column],val);
	}
	
	/** Inserts a double primitive type into the given column */
	public final void set_double(final int column, final double val)
	{
		flags |= (1 << column);
		int[] columnByteOffset = tableCore.columnByteOffset;
		BuffWrite.writeDouble(byteArray, columnByteOffset[column],val);
	}
	
	/** Inserts a char array into the given column. Throws a FemtoDBCharArrayExceedsColumnSizeException if it will not fit */
	public final void set_charArray(final int column, final char[] val) throws FemtoBDCharArrayExceedsColumnSizeException
	{
		int[] columnByteOffset 	= tableCore.columnByteOffset;
		int[] columnByteWidth 	= tableCore.columnByteWidth;
		int columnByteWidth2 = columnByteWidth[column];
		BuffWrite.writeCharArray(byteArray, columnByteOffset[column], val,columnByteWidth2);
		flags |= (1 << column);
	}	
	
}
