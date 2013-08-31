package femtodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import femtodbexceptions.AlteringOperationalTableException;
import femtodbexceptions.InvalidValueException;

public class Table {
	static final int DEFAULT_FILE_SIZE_IN_BYTES		= 3000;
	static final int DEFAULT_CACHE_SIZE_IN_BYTES	= 1000000;
	
	/** The database that contains this table */
	FemtoDB		database;
	
	/** The name of the table, shown in exceptions */
	String 		name;
	
	/** The table number */
	int			tableNumber;
	
	/** The size of the storage files in bytes */
	int			fileSize;
	
	/** The number of rows in each file */
	int			rowsPerFile;
	
	/** Was rowsPerFile set manually */
	boolean		rowsPerFileSet;
	
	/** The cache size. */
	int			cacheSize;
	/** Was the target cache size set manually */
	boolean		cacheSizeSet;
	
	/** The number of files (pages) held within the cache */
	int			cachePages;
	
	/** Has the table been made operational */
	boolean		tableIsOperational;
		
	/** Arrays and values for column meta data */
	String[]	columnNames;
	int[]		columnByteOffset;
	int[]		columnByteWidth;
	int			tableWidth;
	
	/** The next free file number, so the naming of each file is unique */
	long						nextFileNumber;
	
	/** The cache for the table. Not serialised and must be reallocated on loading */
	transient byte[] 			cache;
	
	/** The cache meta data used to handle swapping efficiently. Not serialised */
	transient CacheMetadata[]	cacheMetadata;
	
	/** The file meta data holding what is in each file and its cache status */
	List<FileMetadata>			fileMetadata;
		
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	//                   Constructor
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************	

	Table(String name, int tableNumber, String primaryKeyName)
	{
		this.name			= name;
		this.tableNumber	= tableNumber;
		columnNames 		= new String[0];
		columnByteOffset 	= new int[0];
		columnByteWidth		= new int[0];
		tableWidth = 0;
		
		cacheSizeSet 		= false;
		rowsPerFileSet 		= false;
		tableIsOperational 	= false;
		
		// add primary key column
		if(primaryKeyName == null)primaryKeyName="primarykey";
		addLongColumn(primaryKeyName);

	}
	
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	//                   TABLE MAKE OPERATIONAL METHODS
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************	
	final void setRowsPerFile(int rows)
	{
		if(tableIsOperational) return;
		rowsPerFile = rows;
		rowsPerFileSet = true;
	}
	
	final void setCacheSize(int cacheSize)
	{
		if(tableIsOperational) return;
		this.cacheSize = cacheSize;
		cacheSizeSet = true;
	}
	
	final void makeOperational()throws AlteringOperationalTableException, InvalidValueException
	{
		if(tableIsOperational) throw new AlteringOperationalTableException();
		
		long actualFileSize;
		// Validate rowsPerFile if set otherwise set it automatically to a good value 
		if(rowsPerFileSet)
		{
			if(rowsPerFile <= 0) throw new InvalidValueException("Table " + name + " Files must contain at least one row");
			actualFileSize = rowsPerFile;
			actualFileSize = actualFileSize * tableWidth;
			if(actualFileSize > Integer.MAX_VALUE) throw new InvalidValueException("Table " + name + " Rows per file multiplied table width exceeds an integer value");
		}
		else
		{
			rowsPerFile = DEFAULT_FILE_SIZE_IN_BYTES / tableWidth;
			if (rowsPerFile < 4) rowsPerFile = 4;
			
			actualFileSize = rowsPerFile;
			actualFileSize = actualFileSize * tableWidth;
			if(actualFileSize > Integer.MAX_VALUE)
			{
				rowsPerFile = 2;
				actualFileSize = rowsPerFile;
				actualFileSize = actualFileSize * tableWidth;
			}
		}
		
		// Validate cache size if set otherwise set it automatically to the default value
		if(cacheSizeSet)
		{
			if(cacheSize < actualFileSize)throw new InvalidValueException("Table " + name + " the cache size must be at least the actual file size " + actualFileSize);
		}
		else
		{
			cacheSize = DEFAULT_CACHE_SIZE_IN_BYTES;
		}
		
		// set the fileSize, cachePages and cacheSize to their final values
		fileSize 	= (int)actualFileSize;
		cachePages 	= cacheSize / (int)actualFileSize;
		cacheSize 	= cachePages * (int)actualFileSize;
		
		try{
			cache = new byte[cacheSize];
		}catch(OutOfMemoryError e)
		{
			// re-throw any memory exception providing more information
			throw new OutOfMemoryError("Table " + name + " was unable to allocate its cache of " + cacheSize + " bytes");
		}
		
		// Fill the cache meta data array values with indicate nothing to persist
		cacheMetadata = new CacheMetadata[cachePages];
		for(int x = 0; x < cachePages; x++)
		{
			cacheMetadata[x] = new CacheMetadata(false,-1,-1);
		}
		
		// Initialise nextFileNumber for creating unique filenames
		nextFileNumber = 0;
		
		// Fill the fileMetadata with a single entry referring to an empty file 
		fileMetadata = new ArrayList<FileMetadata>();
		fileMetadata.add(
			new FileMetadata(
				this,
				nextFilenumber(), 
				Long.MIN_VALUE, // ensure the first primary key added falls into this file
				Long.MAX_VALUE,
				Long.MIN_VALUE,	// ensure first value in table inserts after the zero rows
				Long.MIN_VALUE,
				true,			// the new file entry is 'cached' in first cache entry. This cache is then flushed to create the first file
				0, 				// associate with first cache entry
				0				// contains no rows
			)
		);
		
		// associate first cache entry with first fileMetadata entry
		cacheMetadata[0].modified = true;
		cacheMetadata[0].fileMetadataIndex = 0;
		
		// Directly flush the first cache entry to create a file 
		// This is needed in case a shutdown-restart happens before
		// the first entry gets inserted in the table.
		
		//TODO 
//		freeCachePage(0);
	}

	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	//                COLUMN ADDING METHODS
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	
	/** Adds a byte column to the table */
	final void addByteColumn(String columnName)
	{
		if(tableIsOperational) return;
		columnNames = addToArray(columnNames, columnName);
		columnByteOffset = addToArray(columnByteOffset,tableWidth);
		columnByteWidth = addToArray(columnByteWidth,1);
		tableWidth++;
	}
	
	/** Adds a Boolean column to the table, its true width is one byte */
	final void addBooleanColumn(String columnName)
	{
		if(tableIsOperational) return;
		columnNames = addToArray(columnNames, columnName);
		columnByteOffset = addToArray(columnByteOffset,tableWidth);
		columnByteWidth = addToArray(columnByteWidth,1);
		tableWidth++;
	}
	
	/** Adds a byte array column to the table, its true width is width + 4 bytes to encode length*/
	final void addBytesColumn(String columnName, int width) 
	{
		if(tableIsOperational) return;
		columnNames = addToArray(columnNames, columnName);
		columnByteOffset = addToArray(columnByteOffset,tableWidth);
		int trueWidth = 4 + width;
		columnByteWidth = addToArray(columnByteWidth,trueWidth);
		tableWidth += trueWidth;
	}
	
	/** Adds a Short column to the table, its true width is 2 bytes */
	final void addShortColumn(String columnName)
	{
		if(tableIsOperational)return;
		columnNames = addToArray(columnNames, columnName);
		columnByteOffset = addToArray(columnByteOffset,tableWidth);
		columnByteWidth = addToArray(columnByteWidth,2);
		tableWidth += 2;
	}
	
	/** Adds a Character column to the table, its true width is 2 bytes */
	final void addCharColumn(String columnName)
	{
		if(tableIsOperational)return;
		columnNames = addToArray(columnNames, columnName);
		columnByteOffset = addToArray(columnByteOffset,tableWidth);
		columnByteWidth = addToArray(columnByteWidth,2);
		tableWidth += 2;
	}
	
	/** Adds a Integer column to the table, its true width is 2 bytes */
	final void addIntegerColumn(String columnName)
	{
		if(tableIsOperational)return;
		columnNames = addToArray(columnNames, columnName);
		columnByteOffset = addToArray(columnByteOffset,tableWidth);
		columnByteWidth = addToArray(columnByteWidth,4);
		tableWidth += 4;
	}
	
	/** Adds a Long column to the table, its true width is 2 bytes */
	final void addLongColumn(String columnName)
	{
		if(tableIsOperational) return;
		columnNames = addToArray(columnNames, columnName);
		columnByteOffset = addToArray(columnByteOffset,tableWidth);
		columnByteWidth = addToArray(columnByteWidth,8);
		tableWidth += 8;
	}
	
	/** Adds a Float column to the table, its true width is 2 bytes */
	final void addFloatColumn(String columnName)
	{
		if(tableIsOperational) return;
		columnNames = addToArray(columnNames, columnName);
		columnByteOffset = addToArray(columnByteOffset,tableWidth);
		columnByteWidth = addToArray(columnByteWidth,4);
		tableWidth += 4;
	}
	
	/** Adds a Double column to the table, its true width is 2 bytes */
	final void addDoubleColumn(String columnName) 
	{
		if(tableIsOperational) return;
		columnNames = addToArray(columnNames, columnName);
		columnByteOffset = addToArray(columnByteOffset,tableWidth);
		columnByteWidth = addToArray(columnByteWidth,8);
		tableWidth += 8;
	}
	
	/** Adds a Char array column to the table, its true width is width + 2 bytes to encode length*/
	final void addCharsColumn(String columnName, int width)
	{
		if(tableIsOperational) return;
		columnNames = addToArray(columnNames, columnName);
		columnByteOffset = addToArray(columnByteOffset,tableWidth);
		int trueWidth = 2 + width;
		columnByteWidth = addToArray(columnByteWidth,trueWidth);
		tableWidth += trueWidth;
	}
	
	/** Adds a String column to the table, its true width is width + 2 bytes, to encode the length. It is important to note the string gets stored in modified UTF format so the available width in characters may be less than the width parameter */
	final void addStringColumn(String columnName, int width) 
	{
		if(tableIsOperational) return;
		columnNames = addToArray(columnNames, columnName);
		columnByteOffset = addToArray(columnByteOffset,tableWidth);
		int trueWidth = 2 + width;
		columnByteWidth = addToArray(columnByteWidth,trueWidth);
		tableWidth += trueWidth;
	}
	
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	//                PRIVATE METHODS
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	
	private int[] addToArray(int[] in, int toAdd)
	{
		int len = in.length;
		int[] retval = Arrays.copyOf(in, len+1);
		retval[len] = toAdd;
		return retval;
	}
	
	private String[] addToArray(String[] in, String toAdd)
	{
		int len = in.length;
		String[] retval = Arrays.copyOf(in, len+1);
		retval[len] = toAdd;
		return retval;
	}
	
	private long nextFilenumber() {
		return nextFileNumber++;
	}
}
