package femtodb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import femtodbexceptions.AlteringOperationalTableException;
import femtodbexceptions.InvalidValueException;

public class Table {
	static final int 	DEFAULT_FILE_SIZE_IN_BYTES				= 3000;
	static final int 	DEFAULT_CACHE_SIZE_IN_BYTES				= 1000000;
	static final double	DEFAULT_REMOVE_OCCUPANCY_RATIO			= 0.2;
	static final double DEFAULT_ALLOW_COMBINE_OCCUPANCY_RATIO  	= 0.9;
	
	/** The database that contains this table */
	private final FemtoDB		database;
	
	/** The name of the table, shown in exceptions */
	private final String 		name;
	
	/** The table number */
	private int					tableNumber;
	
	/** The size of the storage files in bytes */
	private int					fileSize;
	
	/** The number of rows in each file */
	private int					rowsPerFile;
	
	/** Was rowsPerFile set manually */
	private boolean				rowsPerFileSet;
	
	/** If a file's occupancy ratio is below this value, Should it gets removed
	 * from the cache then the table will attempt to combine it into neighbouring files */
	private double		removeOccupancyRatio;
	
	/** The actual number of occupied rows below which combination into neighbouring files is triggered */
	private int			removeOccupancy;
	
	/** The maximum occupancy ratio a file is permitted to have after a neighbouring file has being combined with it.
	 * This should be less than one to reduce combination-split thrashing */
	private double		combineOccupancyRatio;
	
	/** The actual number of occupied rows a file is permitted to have following a combination with a neighbour */
	private int			combineOccupancy;
	
	/** Has the table been made operational */
	private boolean		tableIsOperational;
	
	/** The next free file number, so the naming of each file is unique */
	private long		nextFileNumber;
	
	// ************ COLUMN INFORMATION *******************
		
	/** Arrays and values for column meta data */
	private String[]	columnNames;
	private int[]		columnByteOffset;
	private int[]		columnByteWidth;
	private int			tableWidth;
	
	// ************ CACHES AND META DATA TABLES **********
	/** The cache size. */
	private int							cacheSize;
	
	/** Was the target cache size set manually */
	private boolean						cacheSizeSet;
	
	/** The number of pages (files) held in the cache */
	private int 						cachePages;
	
	/** The cache for the table. Not serialised and must be reallocated on loading */
	private transient byte[] 			cache;
	
	/** Scratch cache. A small one file cache used during removal of files with low occupancy */
	private transient byte[]			scratchCache;
	
	/** The cache meta data used to handle swapping efficiently. Not serialised */
	private transient CacheMetadata[]	cacheMetadata;
	
	/** The file meta data holding what is in each file and its cache status */
	private List<FileMetadata>			fileMetadata;
		
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	//                   Constructor
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************	

	Table(FemtoDB database, String name, int tableNumber, String primaryKeyName)
	{
		this.database		= database;
		this.name			= name;
		this.tableNumber	= tableNumber;
		columnNames 		= new String[0];
		columnByteOffset 	= new int[0];
		columnByteWidth		= new int[0];
		tableWidth = 0;
		
		cacheSizeSet 		= false;
		rowsPerFileSet 		= false;
		tableIsOperational 	= false;
		
		removeOccupancyRatio 	= DEFAULT_REMOVE_OCCUPANCY_RATIO;
		combineOccupancyRatio 	= DEFAULT_ALLOW_COMBINE_OCCUPANCY_RATIO;
		
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
	
	final void makeOperational()throws InvalidValueException, IOException
	{
		if(tableIsOperational) return;
		tableIsOperational = true;
		
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
		
		// Validate and set remove occupancy
		if(removeOccupancyRatio >= 0.5) throw new InvalidValueException("Table " + name + " removeOccupancyRatio must be set less than 0.5. It was " + removeOccupancyRatio);
		removeOccupancy = (int)(rowsPerFile * removeOccupancyRatio);
		
		// Validate and set combine occupancy
		if(combineOccupancyRatio > 1) throw new InvalidValueException("Table " + name + " combineOccupancyRatio cannot exceed one. It was " + combineOccupancyRatio);
		combineOccupancy = (int)(rowsPerFile * combineOccupancyRatio);
		
		if(combineOccupancy <= removeOccupancy) throw new InvalidValueException("Table " + name + " Resulting removeOccupancy must be less than combineOccupancy for file combining to function correctly. It was " + removeOccupancy + ":" + combineOccupancy);

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
		
		// allocate memory for the cache
		try{
			cache = new byte[cacheSize];
		}catch(OutOfMemoryError e)
		{
			// re-throw any memory exception providing more information
			throw new OutOfMemoryError("Table " + name + " was unable to allocate its cache of " + cacheSize + " bytes");
		}
		
		// allocate memory for the scratch cache
		try{
			scratchCache = new byte[fileSize];
		}catch(OutOfMemoryError e)
		{
			// re-throw any memory exception providing more information
			throw new OutOfMemoryError("Table " + name + " was unable to allocate its scratchCache of " + fileSize + " bytes");
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
		
		// make the directory for the tables files
		String directory = database.path + File.pathSeparator + Integer.toString(tableNumber);
		File f = new File(directory);
		if(!f.mkdir())
		{
			throw new IOException("Table " + name + " was unable to create directory " + directory);
		}
		
		// Directly flush the first cache entry to create a file 
		// This is needed in case a shutdown-restart happens before
		// the first entry gets inserted in the table.
		
		//TODO 
		freeCachePage(0);
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

	public final double getRemoveOccupancyRatio() {
		return removeOccupancyRatio;
	}

	public final void setRemoveOccupancyRatio(double removeOccupancyRatio) {
		if(tableIsOperational) return;
		this.removeOccupancyRatio = removeOccupancyRatio;
	}

	public final double getCombineOccupancyRatio() {
		return combineOccupancyRatio;
	}

	public final void setCombineOccupancyRatio(double combineOccupancyRatio) {
		if(tableIsOperational) return;
		this.combineOccupancyRatio = combineOccupancyRatio;
	}
}
