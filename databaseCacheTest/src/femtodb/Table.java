package femtodb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import femtodbexceptions.InvalidValueException;

public class Table {
	static final int 	DEFAULT_FILE_SIZE_IN_BYTES				= 3000;
	static final int 	DEFAULT_CACHE_SIZE_IN_BYTES				= 1000000;
	static final double	DEFAULT_REMOVE_OCCUPANCY_RATIO			= 0.2;
	static final double DEFAULT_ALLOW_COMBINE_OCCUPANCY_RATIO  	= 0.9;
	static final long	NOT_MODIFIED_LRU_BOOST					= 10;
	static final long	OVER_HALF_FULL_LRU_BOOST				= 5;
	private static final long  PK_CACHE_NOT_SET 				= Long.MIN_VALUE;
	private static final short FLAG_CACHE_NOT_SET 				= Short.MIN_VALUE;
	
	
	/** The database that contains this table */
	final FemtoDB				database;
	
	/** The name of the table, shown in exceptions */
	private final String 		name;
	
	/** The table number */
	int							tableNumber;
	
	/** The size of the storage files in bytes */
	private int					fileSize;
	
	/** The number of rows in each file */
	private int					rowsPerFile;
	
	/** Was rowsPerFile set manually */
	private boolean				rowsPerFileSet;
	
	/** If a file's occupancy ratio is below this value, Should it gets removed
	 * from the cache then the table will attempt to combine it into neighbouring files */
	private double				removeOccupancyRatio;
	
	/** The actual number of occupied rows below which combination into neighbouring files is triggered */
	private int					removeOccupancy;
	
	/** The maximum occupancy ratio a file is permitted to have after a neighbouring file has being combined with it.
	 * This should be less than one to reduce combination-split thrashing */
	private double				combineOccupancyRatio;
	
	/** The actual number of occupied rows a file is permitted to have following a combination with a neighbour */
	private int					combineOccupancy;
	
	/** Has the table been made operational */
	private boolean				tableIsOperational;
	
	/** The next free file number, so the naming of each file is unique */
	private long				nextFileNumber;
	
	// ************ COLUMN INFORMATION *******************
		
	/** Arrays and values for column meta data */
	private String[]			columnNames;
	private int[]				columnByteOffset;
	private int[]				columnByteWidth;
	private int					tableWidth;
	
	// ************ CACHES AND META DATA TABLES **********
	/** The cache size in bytes. */
	private int							cacheSize;
	
	/** Was the target cache size set manually */
	private boolean						cacheSizeSet;
	
	/** The number of pages (files) held in the cache */
	private int 						cachePages;
	
	/** The cache for the table. Not serialised and must be reallocated on loading */
	private transient byte[] 			cache;
	
	/** Contains the primary keys of each row in the cache if extracted, otherwise PK_CACHE_NOT_SET */
	private transient long[]			pkCache;
	
	/** An array containing only PK_CACHE_NOT_SET used to quickly erase old pkCache entries when a cache page gets filled from disk */
	private transient long[]			pkCacheEraser;
	
	/** Contains the primary keys of each row in the cache if extracted, otherwise FLAG_CACHE_NOT_SET */
	private transient short[]			flagCache;
	
	/** An array containing only FLAG_CACHE_NOT_SET used to quickly erase old flagCache entries when a cache page gets filled from disk */
	private transient short[]			flagCacheEraser;
	
	/** Array holding FileMetadata references explaining what is in each cache page, or null if the page is already free */
	private FileMetadata[]				cachePageContents;	
			
	/** The meta data on all the tables files, holding what is in each file and its cache status */
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
		
		// allocate memory for the pkCache
		try{
			int maxRowsInCache = rowsPerFile * cachePages;
			pkCache = new long[(maxRowsInCache)];
			for(int x = 0; x < maxRowsInCache;x++){pkCache[x] = PK_CACHE_NOT_SET;}
		}catch(OutOfMemoryError e)
		{
			// re-throw any memory exception providing more information
			throw new OutOfMemoryError("Table " + name + " was unable to allocate its primary key cache");
		}
		
		// allocate memory for the pkCacheEraser
		try{
			pkCacheEraser = new long[rowsPerFile];
			for(int x = 0; x < rowsPerFile;x++){pkCacheEraser[x] = PK_CACHE_NOT_SET;}
		}catch(OutOfMemoryError e)
		{
			// re-throw any memory exception providing more information
			throw new OutOfMemoryError("Table " + name + " was unable to allocate its primary key cache eraser");
		}

		// allocate memory for the flagCache
		try{
			int maxRowsInCache = rowsPerFile * cachePages;
			flagCache = new short[(maxRowsInCache)];
			for(int x = 0; x < maxRowsInCache;x++){pkCache[x] = FLAG_CACHE_NOT_SET;}
		}catch(OutOfMemoryError e)
		{
			// re-throw any memory exception providing more information
			throw new OutOfMemoryError("Table " + name + " was unable to allocate its flag cache");
		}
		
		// allocate memory for the flagCacheEraser
		try{
			flagCacheEraser = new short[rowsPerFile];
			for(int x = 0; x < rowsPerFile;x++){flagCacheEraser[x] = FLAG_CACHE_NOT_SET;}
		}catch(OutOfMemoryError e)
		{
			// re-throw any memory exception providing more information
			throw new OutOfMemoryError("Table " + name + " was unable to allocate its flag cache eraser");
		}
		
		// set the cache page contents
		cachePageContents = new FileMetadata[cachePages];
		
		// Initialise nextFileNumber for creating unique filenames
		nextFileNumber = 0;
		
		// Fill the fileMetadata with a single entry referring to an empty file 
		fileMetadata = new ArrayList<FileMetadata>();
		
		FileMetadata firstFile = new FileMetadata(
				this,
				nextFilenumber(), 
				Long.MIN_VALUE, // ensure the first primary key added falls into this file
				Long.MAX_VALUE,
				Long.MIN_VALUE,	// ensure first value in table inserts after the zero rows
				Long.MIN_VALUE,
				false,			// the new file entry is 'cached' in first cache entry. This cache is then flushed to create the first file
				0, 				// associate with first cache entry
				0,				// contains no rows
				0L				// modificationServiceNumber initially zero
		);
		
		// make firstFile appear to be loaded into the first cache page and will be written to a file when flushed
		firstFile.cached = true;
		firstFile.cacheIndex = 0;
		firstFile.modified = true;
		
		// add firstFile into fileMetadata list
		fileMetadata.add(firstFile);
		
		// Finally make firstFile appear to be in the cache by completing a cachePageContents entry
		cachePageContents[0] = firstFile;
		
		// make the directory for the tables files to be written to
		String directory = database.path + File.pathSeparator + Integer.toString(tableNumber);
		File f = new File(directory);
		if(!f.mkdir())
		{
			throw new IOException("Table " + name + " was unable to create directory " + directory);
		}
		
		// Directly flush the first cache entry to create a file 
		// This is needed in case a shutdown-restart happens before
		// the first entry gets inserted in the table.
		freeCachePageNoCombine(0, fileMetadata.get(0));
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
	
	//*****************************************************************
	 /**
	 * The main function used by the table for fetching a required file into the cache. It finds the LRU page in the cache, frees it, then uses that page to load the requested file.
	 * @param toLoad		A FileMetadata object indicating which file to load into the cache.
	 * @param allowCombine	When true the method attempts to combine the cached data that was flushed back to disk into neighbouring files. This argument should be set true, unless there is a risk of a cascade of combines occurring. 
	 * @return 				The cache page the file was loaded into.
	 * @throws IOException	Thrown if there was a problem flushing the LRU cache page, or loading the requested file.
	 */
	private final int fetchIntoCache(final FileMetadata toLoad, final boolean allowCombine) throws IOException
	{
		// free a different page in the cache without attempting to combine
		int pageToForceFree = chooseLRU();
		freeCachePage(pageToForceFree,allowCombine);
	
		// cache the file associated with toSave
		fillCachePage(pageToForceFree, toLoad);
		return pageToForceFree;
	}
	// ***********************************************************************************

	/** Ensures a given cache page is made free. If the argument allowCombine is true then the method will attempt to combine the associated file into its neighbours when its occupancy (number of rows) is low. */
	private final void freeCachePage(final int page, final boolean allowCombine) throws IOException
	{
		FileMetadata cacheToFreeFMD = cachePageContents[page];
		if(cacheToFreeFMD == null)return;
		
		// If we are not combining then simply delegate, making no decisions
		int cacheToFreeFMDRows = cacheToFreeFMD.rows;
		int combineOccupancyL = combineOccupancy;
		if((!allowCombine)||(cacheToFreeFMDRows >= combineOccupancyL))
		{
			freeCachePageNoCombine(page, cacheToFreeFMD);
			return;
		}
		
		// Determine what combinations are possible
		List<FileMetadata> fileMetadataL = fileMetadata;
		int cacheToFreeFMDIndex = fileMetadataL.indexOf(cacheToFreeFMD);
		int frontCombinedRows = -1;
		int backCombinedRows = -1;
		boolean frontCombinePossible = false;
		boolean backCombinePossible = false;
		FileMetadata frontFMD = null;
		FileMetadata backFMD = null;
		
		// Check if combination with front is possible
		if(cacheToFreeFMDIndex > 0)
		{
			frontFMD = fileMetadataL.get(cacheToFreeFMDIndex-1);
			frontCombinedRows = cacheToFreeFMDRows + frontFMD.rows;
			if(frontCombinedRows < combineOccupancyL)frontCombinePossible = true;
		}
		
		// Check if combination with back is possible
		int lastIndex = fileMetadataL.size()-1;
		if(cacheToFreeFMDIndex < lastIndex)
		{
			backFMD = fileMetadataL.get(cacheToFreeFMDIndex+1);
			backCombinedRows = cacheToFreeFMDRows + backFMD.rows;
			if(backCombinedRows < combineOccupancyL)backCombinePossible = true;	
		}
		
		// choose free-ing action based on what is possible
		if((!frontCombinePossible)&&(!backCombinePossible))
		{
			freeCachePageNoCombine(page,cacheToFreeFMD);
			return;
		}
		
		if((frontCombinePossible)&&(!backCombinePossible))
		{
			combine(page,cacheToFreeFMD,frontFMD,true);
			return;
		}
		
		if((!frontCombinePossible)&&(backCombinePossible))
		{
			combine(page,cacheToFreeFMD,backFMD,false);
			return;
		}
		
		// If we fall through to here both front and back combines must be possible
		
		// Combine if one of them is already cached
		if((frontFMD.cached)&&(!backFMD.cached))
		{
			combine(page,cacheToFreeFMD,frontFMD,true);
			return;			
		}
		
		if((!frontFMD.cached)&&(backFMD.cached))
		{
			combine(page,cacheToFreeFMD,backFMD,false);
			return;			
		}
		
		// Nether are cached so pick shortest
		if(frontCombinedRows < backCombinedRows)
		{
			combine(page,cacheToFreeFMD,frontFMD,true);
		}
		else
		{
			combine(page,cacheToFreeFMD,backFMD,false);
		}	
	}
	
	/** Free's a given cache page writing its contents to disk. It is generally a last resort called only where there may be a risk of a cascade of combines occurring, for example during the splitting or combining of files. It requires the FileMetadata of the flushed file to also be given as an argument.*/
	private final void freeCachePageNoCombine(int page, FileMetadata fmd) throws IOException
	{
		File f = new File(fmd.filename);
		FileOutputStream fos = new FileOutputStream(f);
		fos.write(cache, (page * fileSize), (tableWidth * fmd.rows));
		fos.flush();
		fos.close();	
		
		// mark the fmd as not cached and the cachePageContents as now free
		fmd.cached = false;
		fmd.cacheIndex = -1;
		cachePageContents[page] = null;
	}
	
	private final void combine(int page, FileMetadata toCombineFMD, FileMetadata targetFMD, boolean isFront) throws IOException
	{
		// ensure frontFMD is cached
		if(!targetFMD.cached)
		{
			fetchIntoCacheIgnoringTheCachePage(targetFMD, page);
		}

		// Execute code specific to front or back combination
		if(isFront)
		{
			combineWithFront(page, toCombineFMD, targetFMD);
		}
		else
		{
			combineWithBack(page, toCombineFMD, targetFMD);
		}
		
		// code common to both front or back combination
		targetFMD.rows = targetFMD.rows + toCombineFMD.rows;
		// use largest modificationServiceNumber
		long toCombineFMDModificationServiceNumber = toCombineFMD.modificationServiceNumber;
		if(toCombineFMDModificationServiceNumber > targetFMD.modificationServiceNumber)targetFMD.modificationServiceNumber = toCombineFMDModificationServiceNumber;
		// use largest lastUsedServiceNumber
		long toCombineFMDLastUsedServiceNumber = toCombineFMD.lastUsedServiceNumber;
		if(toCombineFMDLastUsedServiceNumber > targetFMD.modificationServiceNumber)targetFMD.lastUsedServiceNumber = toCombineFMDLastUsedServiceNumber;	
		targetFMD.modified = true;

		// free up toCombine cache and remove file
		cachePageContents[page] = null;
		File f = new File(toCombineFMD.filename);
		f.delete();
		fileMetadata.remove(toCombineFMD);		
	}
	
	private final void combineWithFront(int page, FileMetadata toCombineFMD, FileMetadata frontFMD)
	{
		frontFMD.upperBound 	= toCombineFMD.upperBound;
		frontFMD.largestPK 		= toCombineFMD.largestPK;
		
		// localise things used several times
		int frontFMDCacheIndex 	= frontFMD.cacheIndex;
		int frontFMDRows 		= frontFMD.rows;
		int toCombineFMDRows 	= toCombineFMD.rows;
		int rowsPerFileL		= rowsPerFile;
		int fileSizeL			= fileSize;
		int tableWidthL			= tableWidth;
		byte[] cacheL			= cache;
		long[] pkCacheL			= pkCache;
		short[] flagCacheL		= flagCache;
		
		// append the rows in toCombine's cache after the rows in front's cache 
		int srcPos = page * fileSizeL;
		int destPos = frontFMDCacheIndex * fileSizeL + tableWidthL * frontFMDRows;
		System.arraycopy(cacheL, srcPos, cacheL, destPos, (toCombineFMDRows * tableWidthL));	
		
		// append pk and flag cache entries after the rows in the front's cache
		int srcPos2 = page * rowsPerFileL;
		int destPos2 = frontFMDCacheIndex * rowsPerFileL + frontFMDRows;
		System.arraycopy(pkCacheL, srcPos2, pkCacheL, destPos2, toCombineFMDRows);
		System.arraycopy(flagCacheL, srcPos2, flagCacheL, destPos2, toCombineFMDRows);
	}
	
	private final void combineWithBack(int page, FileMetadata toCombineFMD, FileMetadata backFMD)
	{
		backFMD.lowerBound 		= toCombineFMD.lowerBound;
		backFMD.smallestPK 		= toCombineFMD.smallestPK;
		
		// localise things used several times
		int backFMDCacheIndex 	= backFMD.cacheIndex;
		int backFMDRows 		= backFMD.rows;
		int toCombineFMDRows 	= toCombineFMD.rows;
		int rowsPerFileL		= rowsPerFile;
		int fileSizeL			= fileSize;
		int tableWidthL			= tableWidth;
		byte[] cacheL			= cache;
		long[] pkCacheL			= pkCache;
		short[] flagCacheL		= flagCache;
		
		// shift the back's cache up to make room
		int srcPos1 	= backFMDCacheIndex * fileSizeL;
		int destPos1 	= srcPos1 + tableWidthL * toCombineFMDRows;
		System.arraycopy(cacheL, srcPos1, cacheL, destPos1, (backFMDRows * tableWidthL));	

		// shift the back's pk and flag cache to make room
		int srcPos2 	= backFMDCacheIndex * rowsPerFileL;
		int destPos2 	= srcPos2 + toCombineFMDRows;
		System.arraycopy(pkCacheL, srcPos2, pkCacheL, destPos2, backFMDRows );	
		System.arraycopy(flagCacheL, srcPos2, flagCacheL, destPos2, backFMDRows );
		
		// insert rows in toCombine's cache into the space made in back's cache 
		int srcPos3		= page * fileSizeL;
		int destPos3	= srcPos1;
		System.arraycopy(cacheL, srcPos3, cacheL, destPos3, (toCombineFMDRows * tableWidthL));	

		// insert rows in toCombine's pk and flag cache into the space made in back's cache 
		int srcPos4 	= page * rowsPerFileL;
		int destPos4	= srcPos2;
		System.arraycopy(pkCacheL, srcPos4, pkCacheL, destPos4, toCombineFMDRows );
		System.arraycopy(flagCacheL, srcPos4, flagCacheL, destPos4, toCombineFMDRows );	
	}
	
	/** Fills a given cache page reading the file referenced by its FileMetadata fmd argument from disk. The page must be made free before this method is called. */
	private final void fillCachePage(int page, FileMetadata fmd) throws IOException
	{
		File f = new File(fmd.filename);
		FileInputStream fis = new FileInputStream(f);
		int bytesToRead = tableWidth * fmd.rows;
		int readByteCount = fis.read(cache, (page * fileSize), bytesToRead);
		if(readByteCount != bytesToRead) throw new IOException("Table " + name + "(" + tableNumber + ") Read incorrect number of bytes from file " + fmd.filename);
		fis.close();
		fmd.cached = true;
		fmd.cacheIndex = page;
		fmd.modified = false;
		cachePageContents[page] = fmd;
		
		// Set pkCache and flagCache entries for page loaded page to NOT_SET.
		// This causes the primary key and flag values to be lazy de-serialised.
		int rowsPerFileL = rowsPerFile;
		int destPos = page * rowsPerFileL;
		System.arraycopy(pkCacheEraser, 0, pkCache, destPos, rowsPerFileL);
		System.arraycopy(flagCacheEraser, 0, flagCache, destPos, rowsPerFileL);
	}
	
	/** Fetches a given file into the cache, returning the cache page that it was fetched into. It is forbidden from using cache page given in the argument pageToExclude. 
	 * It also does not attempt to combine the flushed caches contents with its neighbours. This method is really of special use while combining and splitting files. 
	 * The fetchIntoCache method provides a more general implementation for loading files into the cache. */	
	private final int fetchIntoCacheIgnoringTheCachePage(final FileMetadata toLoad, final int pageToExclude) throws IOException
	{
		// free a different page in the cache without attempting to combine
		int pageToForceFree = chooseLRUExcluding(pageToExclude);
		FileMetadata fileToForceFree = cachePageContents[pageToForceFree];
		freeCachePageNoCombine(pageToForceFree, fileToForceFree);
	
		// cache the file associated with toSave
		fillCachePage(pageToForceFree, toLoad);
		return pageToForceFree;
	}
	
	/** Find the LRU cache page, other than the page given */
	private final int chooseLRUExcluding(final int pageToExclude)
	{
		long bestValueSoFar = Long.MAX_VALUE;
		int bestCandidateSoFar = -1;
		int halfOfRowsPerFile = rowsPerFile >> 1;
		for(int x = 0; x < cachePages; x++)
		{
			if(x == pageToExclude)continue;
			long value = cacheLRURankingAlgorithm(x,halfOfRowsPerFile);
			if(value < bestValueSoFar){bestCandidateSoFar = x; bestValueSoFar = value;}
		}
		return bestCandidateSoFar;
	}
	
	/** Find the LRU cache page */
	private final int chooseLRU()
	{
		long bestValueSoFar = Long.MAX_VALUE;
		int bestCandidateSoFar = -1;
		int halfOfRowsPerFile = rowsPerFile >> 1;
		for(int x = 0; x < cachePages; x++)
		{
			long value = cacheLRURankingAlgorithm(x,halfOfRowsPerFile);
			if(value < bestValueSoFar){bestCandidateSoFar = x; bestValueSoFar = value;}
		}
		return bestCandidateSoFar;
	}
	
	/** Ranking algorithm used to determine best LRU page in cache to swap out */
	private final long cacheLRURankingAlgorithm(int page, int halfOfRowsPerFile)
	{
		FileMetadata fmd = cachePageContents[page];
		if(fmd == null)return Long.MIN_VALUE; // perfect an unused page :-)
		long retval = fmd.lastUsedServiceNumber;
		if(fmd.modified) retval -= NOT_MODIFIED_LRU_BOOST;
		if(fmd.rows > halfOfRowsPerFile)retval -= OVER_HALF_FULL_LRU_BOOST;
		return retval;
	}
	
	/** Returns a new int array consisting of the passed in array appended with the passed in value */
	private int[] addToArray(int[] in, int toAdd)
	{
		int len = in.length;
		int[] retval = Arrays.copyOf(in, len+1);
		retval[len] = toAdd;
		return retval;
	}

	/** Returns a new String array consisting of the passed in array  appended with the passed in value */
	private String[] addToArray(String[] in, String toAdd)
	{
		int len = in.length;
		String[] retval = Arrays.copyOf(in, len+1);
		retval[len] = toAdd;
		return retval;
	}
	
	/** Returns the next available file number for the table that has never been used */
	private long nextFilenumber() {
		return nextFileNumber++;
	}
	
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	//                FIELD  GET / SET METHODS
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	
	final double getRemoveOccupancyRatio() {
		return removeOccupancyRatio;
	}

	final void setRemoveOccupancyRatio(double removeOccupancyRatio) {
		if(tableIsOperational) return;
		this.removeOccupancyRatio = removeOccupancyRatio;
	}

	final double getCombineOccupancyRatio() {
		return combineOccupancyRatio;
	}

	final void setCombineOccupancyRatio(double combineOccupancyRatio) {
		if(tableIsOperational) return;
		this.combineOccupancyRatio = combineOccupancyRatio;
	}
}
