package femtodb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.naming.OperationNotSupportedException;

import femtodbexceptions.InvalidValueException;

public class Table {
	static final int 			DEFAULT_FILE_SIZE_IN_BYTES				= 3000;
	static final int 			DEFAULT_CACHE_SIZE_IN_BYTES				= 1000000;
	static final double			DEFAULT_REMOVE_OCCUPANCY_RATIO			= 0.2;
	static final double 		DEFAULT_ALLOW_COMBINE_OCCUPANCY_RATIO  	= 0.9;
	static final long			NOT_MODIFIED_LRU_BOOST					= 10;
	static final long			OVER_HALF_FULL_LRU_BOOST				= 5;
	private static final long  	PK_CACHE_NOT_SET 						= Long.MAX_VALUE;
	private static final short 	FLAG_CACHE_NOT_SET 						= Short.MIN_VALUE;
	
	
	/** The database that contains this table */
	final FemtoDB				database;
	
	/** The name of the table, shown in exceptions */
	private final String 		name;
	
	/** The table number */
	int							tableNumber;
	
	/** The directory the tables files will be put in. Declared transient so it adapts to operating system seperators */
	private transient String	tableDirectory;
	
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
	private FileMetadata[]				cacheContents;	
			
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
		addShortColumn("femto_db_status");
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
	
	//******************************************************
	//******************************************************
	//         END OF COLUMN ADDING METHODS
	//******************************************************
	//******************************************************

	
	
	
	
	
	
	
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
	
	
	// *********************************************
	//          MAKE OPERATIONAL
	// *********************************************
	/** Allocates memory and creates first file making the table operational */
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
		cacheContents = new FileMetadata[cachePages];
		
		// Initialise nextFileNumber for creating unique filenames
		nextFileNumber = 0;
		
		// Create the directory
		tableDirectory = database.path + File.separator + Integer.toString(tableNumber);
		File f = new File(tableDirectory);
		if(f.exists()){recursiveDelete(f);}
		if(!f.mkdir())
		{
			throw new IOException("Table " + name + " was unable to create directory " + tableDirectory);
		}
		
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
		cacheContents[0] = firstFile;
		
		// Directly flush the first cache entry to create a file 
		// This is needed in case a shutdown-restart happens before
		// the first entry gets inserted in the table.
		freeCachePage(0);
	}	
	
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	//     END OF TABLE MAKE OPERATIONAL METHODS
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************		
	
	
	//******************************************************
	//******************************************************
	//      START OF LOADING INTO AND FREEING CACHE CODE
	//******************************************************
	//******************************************************	
	
	 /**
	 * The main function used by the table for fetching a required file into the cache. It finds the LRU page in the cache, frees it, then uses that page to load the requested file.
	 * @param toLoad		A FileMetadata object indicating which file to load into the cache.
	 * @return 				The cache page the file was loaded into.
	 * @throws IOException	Thrown if there was a problem flushing the LRU cache page, or loading the requested file.
	 */
	private final int loadFileIntoCache(final FileMetadata toLoad) throws IOException
	{		
		// free a different page in the cache without attempting to combine
		int pageToForceFree = findLRUCachePage();
		freeCachePage(pageToForceFree);
	
		// cache the file associated with toSave
		loadFileIntoCachePage(pageToForceFree, toLoad);
		return pageToForceFree;
	}
	
	/** Fills a given cache page reading the file referenced by its FileMetadata fmd argument from disk. The page must be made free before this method is called. */
	private final void loadFileIntoCachePage(int page, FileMetadata fmd) throws IOException
	{
		// load the file
		File f = new File(fmd.filename);
		FileInputStream fis = new FileInputStream(f);
		int bytesToRead = tableWidth * fmd.rows;
		int readByteCount = fis.read(cache, (page * fileSize), bytesToRead);
		if(readByteCount != bytesToRead) throw new IOException("Table " + name + "(" + tableNumber + ") Read incorrect number of bytes from file " + fmd.filename);
		fis.close();
		
		// update fmd
		fmd.cached = true;
		fmd.cacheIndex = page;
		fmd.modified = false;
		
		// add to cacheContents
		cacheContents[page] = fmd;
		
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
	private final int loadFileIntoCacheIgnoringGivenCachePage(final FileMetadata toLoad, final int pageToExclude) throws IOException
	{
		// free a different page in the cache without attempting to combine
		int pageToForceFree = findLRUCachePageExcludingPage(pageToExclude);
		freeCachePage(pageToForceFree);
	
		// cache the file associated with toSave
		loadFileIntoCachePage(pageToForceFree, toLoad);
		return pageToForceFree;
	}
	
	/** Find the LRU cache page, other than the page given */
	private final int findLRUCachePageExcludingPage(final int pageToExclude)
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
	private final int findLRUCachePage()
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
		FileMetadata fmd = cacheContents[page];
		if(fmd == null)return Long.MIN_VALUE; // perfect an unused page :-)
		long retval = fmd.lastUsedServiceNumber;
		if(fmd.modified) retval -= NOT_MODIFIED_LRU_BOOST;
		if(fmd.rows > halfOfRowsPerFile)retval -= OVER_HALF_FULL_LRU_BOOST;
		return retval;
	}
	
	/** Free's a given cache page writing its contents to disk. */
	private final void freeCachePage(int page) throws IOException
	{	
		FileMetadata fmd = cacheContents[page];
		if(fmd == null)return; // cache page must already be free
		File f = new File(fmd.filename);
		FileOutputStream fos = new FileOutputStream(f);
		fos.write(cache, (page * fileSize), (tableWidth * fmd.rows));
		fos.flush();
		fos.close();	
		
		// mark the fmd as not cached and the cachePageContents as now free
		fmd.cached = false;
		fmd.cacheIndex = -1;
		cacheContents[page] = null;
	}
	
	//******************************************************
	//******************************************************
	//      END OF LOADING INTO AND FREEING CACHE CODE
	//******************************************************
	//******************************************************
	
	
	
	//******************************************************
	//******************************************************
	//         START OF COMBINE CODE
	//******************************************************
	//******************************************************	
	
	/** Attempts to combine the file related to a given cache page into its neighbours, freeing the cache page*/
	private final void tryToCombine(final int page, FileMetadata cacheToFreeFMD) throws IOException
	{		
		// preconditions are the page is loaded in the cache, it is not already set free and it is modified
		System.out.println("trying combine");

		// See if combine is worth taking any further
		int cacheToFreeFMDRows = cacheToFreeFMD.rows;
		if(cacheToFreeFMDRows >= removeOccupancy)
		{
			System.out.println("cacheToFreeFMDRows >= removeOccupancy");	
			return; // we cannot combine so simply return
		}
		
		// Determine what combinations are possible
		List<FileMetadata> fileMetadataL = fileMetadata;
		int cacheToFreeFMDIndex = fileMetadataL.indexOf(cacheToFreeFMD);
		int combineOccupancyL = combineOccupancy;
		int frontCombinedRows = -1;
		int backCombinedRows = -1;
		boolean frontCombinePossible = false;
		boolean backCombinePossible = false;
		FileMetadata frontFMD = null;
		FileMetadata backFMD = null;
		
		// Check if combination with front is possible
		if(cacheToFreeFMDIndex > 0)
		{
			System.out.println("file has a front");
			frontFMD = fileMetadataL.get(cacheToFreeFMDIndex-1);
			frontCombinedRows = cacheToFreeFMDRows + frontFMD.rows;
			if(frontCombinedRows < combineOccupancyL)frontCombinePossible = true;
		}
		
		// Check if combination with back is possible
		int lastIndex = fileMetadataL.size()-1;
		if(cacheToFreeFMDIndex < lastIndex)
		{
			System.out.println("file has a back");
			backFMD = fileMetadataL.get(cacheToFreeFMDIndex+1);
			backCombinedRows = cacheToFreeFMDRows + backFMD.rows;
			if(backCombinedRows < combineOccupancyL)backCombinePossible = true;	
		}
		
		System.out.println("Front combine possible:" + frontCombinePossible);
		System.out.println("Back combine possible:" + backCombinePossible);		

		// choose free-ing action based on what is possible
		if((!frontCombinePossible)&&(!backCombinePossible))
		{
			freeCachePage(page);
			return;
		}
		
		if((frontCombinePossible)&&(!backCombinePossible))
		{
			combineStage2(page,cacheToFreeFMD,frontFMD,true);
			return;
		}
		
		if((!frontCombinePossible)&&(backCombinePossible))
		{
			combineStage2(page,cacheToFreeFMD,backFMD,false);
			return;
		}
		
		// If we fall through to here both front and back combines must be possible
		
		// Combine favouring which one is already cached
//		if((frontFMD.cached)&&(!backFMD.cached))
//		{
//			combineStage2(page,cacheToFreeFMD,frontFMD,true);
//			return;			
//		}
//		
//		if((!frontFMD.cached)&&(backFMD.cached))
//		{
//			combineStage2(page,cacheToFreeFMD,backFMD,false);
//			return;			
//		}

		
		// Nether are cached so pick shortest
		if(frontCombinedRows < backCombinedRows)
		{
			combineStage2(page,cacheToFreeFMD,frontFMD,true);
		}
		else
		{
			combineStage2(page,cacheToFreeFMD,backFMD,false);
		}	
	}
	
	/** Combines a given cached file, on a given cache page, into one of its neighbours */
	private final void combineStage2(int page, FileMetadata toCombineFMD, FileMetadata targetFMD, boolean isFront) throws IOException
	{
		// ensure targetFMD is cached
		if(!targetFMD.cached)
		{
			loadFileIntoCacheIgnoringGivenCachePage(targetFMD, page);
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
		cacheContents[page] = null;
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
	
	//******************************************************
	//******************************************************
	//         END OF COMBINE CODE
	//******************************************************
	//******************************************************	
	
	
	//******************************************************
	//******************************************************
	//         START OF SPLITTING CODE
	//******************************************************
	//******************************************************	

	/** Called when a page becomes full. It is split in half generating a new file and fileMetadata entry. */
	private final void splitFile(int page, FileMetadata fmd) throws IOException
	{
		int newRowsInFirst 					= fmd.rows >> 1; // note this is also the index of first row in second
		int newIndexOfLastRowInFirst 		= newRowsInFirst - 1; 
		int newRowsInSecond 				= rowsPerFile - newRowsInFirst;		
		long primaryKeyOfLastRowInFirst 	= getPrimaryKeyForCacheRow(page,newIndexOfLastRowInFirst);	
		long primaryKeyOfFirstRowInSecond 	= getPrimaryKeyForCacheRow(page,newRowsInFirst);
		long lastPrimaryKeyRowInSecond 		= fmd.largestPK;
		long upperBoundOfSecond 			= fmd.upperBound;
		
		// correct fmd for the split
		fmd.largestPK 	= primaryKeyOfLastRowInFirst;
		fmd.upperBound 	= primaryKeyOfFirstRowInSecond;
		fmd.rows 		= newRowsInFirst;
		
		// create metadata for the second file
		FileMetadata secondFile = new FileMetadata(
				this,
				nextFilenumber(), 
				primaryKeyOfFirstRowInSecond,
				upperBoundOfSecond,
				primaryKeyOfFirstRowInSecond,
				lastPrimaryKeyRowInSecond,
				false, 	// not cached		
				0, 		// dont care
				newRowsInSecond,
				fmd.modificationServiceNumber
		);
		
		int indexInFMDTable = fileMetadata.indexOf(fmd);
		fileMetadata.add((indexInFMDTable+1), secondFile);
		
		// create the second file
		File f = new File(secondFile.filename);
		FileOutputStream fos = new FileOutputStream(f);
		fos.write(cache, (page * fileSize) + (newRowsInFirst * tableWidth), (newRowsInSecond * tableWidth));
		fos.flush();
		fos.close();	
	}
	
	//******************************************************
	//******************************************************
	//         END OF SPLITTING CODE
	//******************************************************
	//******************************************************	

	
	
	
	
	
	
	
	//******************************************************
	//******************************************************
	//         START OF INSERT CODE
	//******************************************************
	//******************************************************	
	
	final boolean insertByPrimaryKey(long primaryKey, byte[] toInsert, long serviceNumber) throws IOException
	{
		int fileMetadataListIndex 	= fileMetadataBinarySearch(primaryKey);		
		FileMetadata fmd 			= fileMetadata.get(fileMetadataListIndex);
		int fmdRows 				= fmd.rows;

		// Ensure the file containing the range the primary key falls in is loaded into the cache
		int page = 0;
		if(fmd.cached)
		{
			page = fmd.cacheIndex;
		}
		else
		{
			page = loadFileIntoCache(fmd);
		}
		
		// handle the special case of an empty table
		if(fmdRows == 0)
		{
			insertIntoEmptyPage(primaryKey, toInsert, serviceNumber, page, fmd);
			return true;
		}
		
		// Find the insert point
		int insertRow = 0;
		if(primaryKey < fmd.smallestPK)
		{
			insertRow = 0;
		}
		else
		{
			// Find the row immediately preceding the primary key 
			insertRow = primaryKeyBinarySearch(page, primaryKey, true,true);		
			if(insertRow == -1)return false; // primary key already in use
			insertRow++; // we need to start shifting from the next one			
		}
		
		// localise class fields for speed
		byte[]	cacheL				= cache;
		long[] 	pkCacheL 			= pkCache;
		short[] flagCacheL 			= flagCache;
		int 	tableWidthL 		= tableWidth;
		
		// calculate the pages start indexes
		int 	pkCachePageStart 	= page * rowsPerFile;
		int		cachePageStart 		= page * fileSize;
		
		// make room for the insert into pkCache and flagCache
		int srcPos1 				= pkCachePageStart + insertRow;
		System.arraycopy(pkCacheL, srcPos1, pkCacheL, srcPos1, (fmdRows - insertRow) );
		System.arraycopy(flagCacheL, srcPos1, flagCacheL, srcPos1, (fmdRows - insertRow) );
		
		// make room for the insert into the cache
		int srcPos2 				= cachePageStart + insertRow * tableWidthL;
		System.arraycopy(cacheL, srcPos2, cacheL, (srcPos2 + tableWidthL), (fmdRows - insertRow) * tableWidthL);

		// perform the insert
		pkCacheL[srcPos1] 			= primaryKey;
		flagCacheL[srcPos1]			= FLAG_CACHE_NOT_SET;
		System.arraycopy(toInsert, 0, cacheL, srcPos2, tableWidthL);
		
		// update file meta data
		if(primaryKey > fmd.largestPK)	fmd.largestPK 	= primaryKey;
		if(primaryKey < fmd.smallestPK)	fmd.smallestPK 	= primaryKey;
		fmd.lastUsedServiceNumber 		= serviceNumber;
		fmd.modificationServiceNumber 	= serviceNumber;
		fmd.modified					= true;
		fmd.rows++;
		
		// split the file when it is full, so that new inserts cannot cause it to pop !
		if(fmd.rows == rowsPerFile)splitFile(page, fmd);
		return true;
	}	
	

	
	final void insertIntoEmptyPage(long primaryKey, byte[] toInsert, long serviceNumber, int page, FileMetadata fmd) throws IOException
	{
		int 	pkCachePageStart 		= page * rowsPerFile;
		int		cachePageStart 			= page * fileSize;
		
		// perform the insert
		pkCache[pkCachePageStart] 		= primaryKey;
		flagCache[pkCachePageStart]		= FLAG_CACHE_NOT_SET;
		System.arraycopy(toInsert, 0, cache, cachePageStart, tableWidth);
		
		// update the file meta data
		fmd.largestPK 					= primaryKey;
		fmd.smallestPK 					= primaryKey;
		fmd.lastUsedServiceNumber 		= serviceNumber;
		fmd.modificationServiceNumber 	= serviceNumber;
		fmd.modified					= true;
		fmd.rows++;
	}
	
	
	//******************************************************
	//******************************************************
	//         END OF INSERT CODE
	//******************************************************
	//******************************************************
	
	
	
	

	//******************************************************
	//******************************************************
	//         START OF SEEK CODE
	//******************************************************
	//******************************************************	
	
	final byte[] seekByPrimaryKey(long primaryKey, long serviceNumber) throws IOException
	{
		System.out.println("seeking " + primaryKey);
		int fileMetadataListIndex = fileMetadataBinarySearch(primaryKey);
		System.out.println("is in metafile index " + fileMetadataListIndex);
		// ensure the file containing the range the primary key falls in is loaded into the cache
		FileMetadata fmd = fileMetadata.get(fileMetadataListIndex);
		int page = 0;
		if(fmd.cached)
		{
			page = fmd.cacheIndex;
		}
		else
		{
			page = loadFileIntoCache(fmd);
		}
		
		if(fmd.rows == 0)
		{
				System.out.println("empty table!");
				return null;	// empty table!		
		}
		int row = primaryKeyBinarySearch(page, primaryKey, false, false);
		if(row == -1)
			{
				System.out.println("primary key not found!");
				return null;		// primary key not found
			}
		byte[] retval = new byte[tableWidth];
		
		int srcPos = page * fileSize + row * tableWidth;	
		System.arraycopy(cache, srcPos, retval, 0, tableWidth);
		fmd.lastUsedServiceNumber 		= serviceNumber;
		return retval;
	}
	
	//******************************************************
	//******************************************************
	//         END OF SEEK CODE
	//******************************************************
	//******************************************************	
	

	
	
	
	
	//******************************************************
	//******************************************************
	//         START OF DELETE ROW CODE
	//******************************************************
	//******************************************************	
	
	/** Deletes a row in the table given its primary key and a serviceNumber for the operation */
	final boolean deletePrimaryKey(long primaryKey, long serviceNumber) throws IOException
	{
		int fileMetadataListIndex = fileMetadataBinarySearch(primaryKey);
		
		// ensure the file containing the range the primary key falls in is loaded into the cache
		FileMetadata fmd = fileMetadata.get(fileMetadataListIndex);
		int page = 0;
		if(fmd.cached)
		{
			page = fmd.cacheIndex;
		}
		else
		{
			page = loadFileIntoCache(fmd);
		}
		
		if(fmd.rows == 0)return false;	// empty table!		
		int row = primaryKeyBinarySearch(page, primaryKey, false, false);
		if(row == -1)return false;		// primary key not found
		
		deleteRow(primaryKey,page,row,serviceNumber);
		return true;
	}
	
	final void deleteRow(long primaryKey, int page, int row, long serviceNumber) throws IOException
	{
		FileMetadata fmd = cacheContents[page];
		int fmdRows = fmd.rows;
		
		if(fmdRows == 1)
		{
			// special case of emptying table
			fmd.smallestPK 	= Long.MIN_VALUE;
			fmd.largestPK 	= Long.MAX_VALUE;
		}
		else
		{
			if(primaryKey == fmd.smallestPK)
			{
				fmd.smallestPK = getPrimaryKeyForCacheRow(page, (row+1));
			}
			if(primaryKey == fmd.largestPK)
			{
				fmd.largestPK = getPrimaryKeyForCacheRow(page, (row-1));
			}
		}
		
		// Shift following pk and flag cache rows up over deleted row
		int destPos1 = page * rowsPerFile + row;
		int srcPos1 = destPos1 + 1;
		int length1 = (fmdRows - 1) - row;
		System.arraycopy(pkCache, srcPos1, pkCache, destPos1, length1);
		System.arraycopy(flagCache, srcPos1, flagCache, destPos1, length1);
		
		// Shift following cache rows up over deleted row
		int tableWidthL = tableWidth;
		int destPos2 = page * fileSize + row * tableWidthL;
		int srcPos2 = destPos2 + tableWidthL;
		int length2 = length1 * tableWidthL;
		System.arraycopy(cache, srcPos2, cache, destPos2, length2);
		
		// update the remaining fileMetadata
		fmd.lastUsedServiceNumber 		= serviceNumber;
		fmd.modificationServiceNumber 	= serviceNumber;
		fmd.modified = true;
		fmd.rows--;
		
		// try to combine with neighbours
		tryToCombine(page,fmd);
	}
	
	//******************************************************
	//******************************************************
	//         END OF DELETE ROW CODE
	//******************************************************
	//******************************************************	
	
	
	
	
	
	//******************************************************
	//******************************************************
	//         START OF GENERIC PRIMARY KEY SEARCHING CODE
	//******************************************************
	//******************************************************	

	
	/** Returns the fileMetadata list index for a FileMetadata object that contains the given primary key */ 
	private final int fileMetadataBinarySearch(final long primaryKey)
	{
		final List<FileMetadata> treeL = fileMetadata;
		int minIndex 	= 0;
		int maxIndex 	= treeL.size();
		
		int testIndex 	= (maxIndex + minIndex) >> 1;
		FileMetadata bte = treeL.get(testIndex);
		boolean toBig 	= (primaryKey < bte.lowerBound);
		boolean toSmall = (primaryKey >= bte.upperBound);
		
		while( toBig || toSmall )
		{
			if(toBig)
			{
				maxIndex = testIndex;		
			}
			else
			{
				minIndex = testIndex;
			}
			testIndex 	= (maxIndex + minIndex) >> 1;
			bte 		= treeL.get(testIndex);
			toBig 		= (primaryKey < bte.lowerBound);
			toSmall 	= (primaryKey >= bte.upperBound);	
		}
		return testIndex;		
	}
	
	/** Returns the cache index for a given primary key. Requires the index for the containing file in the fileMetadata list. */
	private final int primaryKeyBinarySearch(final int page, final long primaryKey, boolean forInsert, boolean overwritePrevention)
	{
		FileMetadata fmd = cacheContents[page];
		
		int minIndex = 0;
		int maxIndex = fmd.rows;
//		System.out.println("number of rows " + maxIndex);
		
		int testIndex 	= (maxIndex + minIndex) >> 1;
//		System.out.println("test index= " + testIndex);
		long testPK 	= getPrimaryKeyForCacheRow(page, testIndex);
//		System.out.println("returned primary key for row " + testPK);
		boolean toBig 	= (primaryKey < testPK);
		boolean toSmall = (primaryKey > testPK);
		long priorIndex = Integer.MIN_VALUE;
		while( toBig || toSmall )
		{
			if(toBig)
			{
				maxIndex = testIndex;		
			}
			else
			{
				minIndex = testIndex;
			}
			
			testIndex 	= (maxIndex + minIndex) >> 1;
//			System.out.println("test index= " + testIndex);
			
			if(testIndex == priorIndex)
			{
				// we stopped moving!
				if(forInsert){return testIndex;}else{return -1;}
			}
			priorIndex = testIndex;
			
			testPK 		= getPrimaryKeyForCacheRow(page, testIndex);
			System.out.println("returned primary key for row " + testPK);
			toBig 	= (primaryKey < testPK);
			toSmall = (primaryKey > testPK);
		}
		if(overwritePrevention)return -1;
		return testIndex;		
	}
	
	
	//******************************************************
	//******************************************************
	//         END OF GENERIC PRIMARY KEY SEARCHING CODE
	//******************************************************
	//******************************************************	
	
	//******************************************************
	//******************************************************
	//        START OF ITERATORS
	//******************************************************
	//******************************************************
	FemtoDBIterator fastIterator()
	{
		return (new FemtoDBIterator()
				{
					FileMetadata fmd = null;
					int fmdRows;
					int currentRow;
					
					boolean 		hasNextCalledLast 		= false;
					boolean 		hasNextCalledLastResult = false;
					
					@Override
					public boolean hasNext() {					
						// if next() was not called since last call, then send previous result
						if(hasNextCalledLast)return hasNextCalledLastResult;
						List<FileMetadata> fileMetadataL = fileMetadata;
						
						// set these local variables to align with last call to next(), if one occured
						FileMetadata hasNextFMD = fmd;
						int hasNextRows 		= fmdRows;
						int hasNextCurrentRow 	= currentRow;
						
						// update fields needed to look forward from 
						if(hasNextFMD == null)
						{
							hasNextFMD			= fileMetadataL.get(0);
							hasNextRows 		= hasNextFMD.rows;
							hasNextCurrentRow 	= -1;
						}
						
						// try stepping forward
						hasNextCurrentRow++;
						
						// if we have another row in hasNextFMD return true
						if(hasNextCurrentRow < hasNextRows)
						{
							hasNextCalledLast 		= true;
							hasNextCalledLastResult = true;									
							return true;							
						}
						
						// Seek forward through fileMetadata list for next row
						int nextFMDIndex = fileMetadataL.indexOf(hasNextFMD);
						int fileMetadataSize = fileMetadataL.size();
						while(true)
						{
							nextFMDIndex++;
							if(nextFMDIndex == fileMetadataSize)
							{
								// reached end of list so return false
								hasNextCalledLast 		= true;
								hasNextCalledLastResult = false;									
								return false;
							}
							hasNextFMD = fileMetadataL.get(nextFMDIndex);
							
							// if it has a row return true
							hasNextRows = hasNextFMD.rows;
							if(hasNextRows > 0)
							{
								hasNextCalledLast 		= true;
								hasNextCalledLastResult = true;									
								return true;								
							}
						}							
					}

					@Override
					public RowAccessType next() {
						hasNextCalledLast = false;
						List<FileMetadata> fileMetadataL = fileMetadata;
						// update fields needed to look forward from 
						if(fmd == null)
						{
							fmd			= fileMetadataL.get(0);
							fmdRows 	= fmd.rows;
							currentRow 	= -1;
						}
						
						// try stepping forward
						currentRow++;
						
						// if we have another row in hasNextFMD return true
						if(currentRow < fmdRows)
						{
							try {
								return getRowAccessType(fmd, currentRow);
							} catch (IOException e) {
								return null;
							}
						}
						
						// Seek forward through fileMetadata list for next row
						int nextFMDIndex = fileMetadataL.indexOf(fmd);
						int fileMetadataSize = fileMetadataL.size();
						while(true)
						{
							nextFMDIndex++;
							if(nextFMDIndex == fileMetadataSize)
							{
								// reached end of list so return null								
								return null;
							}
							fmd = fileMetadataL.get(nextFMDIndex);
							fmdRows = fmd.rows;
							if(fmdRows > 0)
							{
								currentRow = 0;
								try {
									return getRowAccessType(fmd, currentRow);
								} catch (IOException e) {
									return null;
								}
							}
						}	
					}

					@Override
					public void remove() {
						// We don't support remove() because it may alter the fileMetadata table, corrupting the iterator
						throw new UnsupportedOperationException();
					}
					
					@Override
					public void reset() 
					{
						fmd = null;
						hasNextCalledLast 		= false;
						hasNextCalledLastResult = false;
					}
					
					/** Private method used by Fast Iterator only */
					private RowAccessType getRowAccessType(FileMetadata fmd, int row) throws IOException
					{
						int page;
						if(fmd.cached)
						{
							page = fmd.cacheIndex;
						}
						else
						{
							page = loadFileIntoCache(fmd);
							// update the service number so it remains cached
							fmd.lastUsedServiceNumber = Table.this.database.getServiceNumber();
						}
						
						byte[] returnData = new byte[tableWidth];
						int srcPos = page * fileSize + row * tableWidth;
						System.arraycopy(cache, srcPos, returnData, 0, tableWidth);
						long primaryKey = getPrimaryKeyForCacheRow(page, row);
						RowAccessType retval = new RowAccessType(primaryKey, Table.this, returnData);
						return retval;		
					}
				});
		
	}
	
	//******************************************************
	//******************************************************
	//        END OF ITERATORS
	//******************************************************
	//******************************************************
	
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	//                SEVERAL PRIVATE METHODS
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************

	private long getPrimaryKeyForCacheRow(int page,int row)
	{
		// try the pkCache
		int pkCacheIndex = page * rowsPerFile + row;
		long pkCacheResult = pkCache[pkCacheIndex];
		if(pkCacheResult != PK_CACHE_NOT_SET)
			{
//				System.out.println("pkcache res = " + pkCacheResult);
				return pkCacheResult;
			}
		
		// get from cache bytes
		int cacheIndex = (page * fileSize) + (tableWidth * row);
		long deserialisedResult = BuffRead.readLong(cache, cacheIndex);
		pkCache[pkCacheIndex] = deserialisedResult;
//		System.out.println("des res = " + deserialisedResult);
		return deserialisedResult;
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
	
	/** Recursively deletes a file or directory */
	private void recursiveDelete(File f)
	{
		if(f != null)
		{
			if(f.exists())
			{
				if(f.isDirectory())
				{
					File[] files = f.listFiles();
					for(File file: files)
					{
						recursiveDelete(file);
					}
					f.delete();
				}
				else
				{
					f.delete();
				}
			}
		}
	}
	
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	//         END OF VARIOUS PRIVATE METHODS
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	
	
	
	
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	//         METHODS FOR TEST AND DEBUG
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	
	public String toString()
	{
		String retval = "";
		retval = retval + "name: " 			+ name + "\n";
		retval = retval + "tableNumber: " 	+ tableNumber + "\n";
		retval = retval + "fileSize: " 		+ fileSize + "\n";
		retval = retval + "rowsPerFile: " 	+ rowsPerFile + "\n";
		retval = retval + "removeOccupancyRatio: " 	+ removeOccupancyRatio + "\n";
		retval = retval + "removeOccupancy: " 		+ removeOccupancy + "\n";
		retval = retval + "combineOccupancyRatio: " + combineOccupancyRatio + "\n";
		retval = retval + "combineOccupancy: " 		+ combineOccupancy + "\n";
		retval = retval + "nextFileNumber: " 		+ nextFileNumber + "\n";
		retval = retval + "cacheSize: " + cacheSize + "\n";
		retval = retval + "cachePages: " + cachePages + "\n";
		int len = columnNames.length;
		retval = retval + ".......\n";
		for(int x = 0 ; x < len; x++)
		{
			retval = retval + columnNames[x] + " off=" + columnByteOffset[x] + " wid=" + columnByteWidth[x] + "\n";
 		}
		retval = retval + "tableWidth: " + tableWidth + "\n";
		retval = retval + ".......\n";
		
		retval = retval + "******** File Metadata ****** \n";
		int len2 = fileMetadata.size();
		for(int y = 0; y < len2; y++)
		{
			retval = retval + " - - - - - - - - - - - -  \n";
			retval = retval + fileMetadata.get(y).toString() + "\n";
			retval = retval + " - - - - - - - - - - - -  \n";
		}
		
		retval = retval + "******** Cache contents ****** \n";
		
		for(int z = 0; z < cachePages; z++)
		{
			retval = retval + Integer.toString(z) + "   ";
			FileMetadata f = cacheContents[z];
			if(f == null){retval = retval + "null\n";}else{retval = retval + f.filenumber + "\n";}
		}
		retval = retval + "******************************** \n";
		retval = retval + "******************************** \n";
		return retval;
	}

	public String cacheToString()
	{
		int cachePagesL 	= cachePages;
		int rowsPerFileL 	= rowsPerFile;
		int fileSizeL 		= fileSize;
		int tableWidthL 	= tableWidth;
		String retval = "";
		for(int page = 0; page < cachePagesL; page ++)
		{
			retval = retval + "=========== page " + page + " =================== \n";
			for(int row = 0; row < rowsPerFile; row++)
			{
				int pkCacheRow = page * rowsPerFileL + row;
				retval = retval + fwid(Long.toString(pkCache[pkCacheRow]),20) + "|";
				retval = retval + fwid(Short.toString(flagCache[pkCacheRow]),6) + "|";
				int cacheIndex = page * fileSizeL + row * tableWidthL;
				retval = retval + bytesToString(cache, cacheIndex, tableWidth);
				retval = retval + "\n";
			}
		}
		return retval;
	}
	
	private String fwid(String in, int len)
	{
		int inlen = in.length();
		String retval = "";
		int toAdd = len - inlen;
		if (toAdd > 0)
		{
			for(int y = 0; y < toAdd; y++) retval = retval + " ";
		}
		retval = retval + in;
		return retval;
	}
	
	private String bytesToString(byte[] bytes, int offset, int length)
	{
		String hex = "0123456789ABCDEF";
		String retval = "";
		for(int x = 0; x < length; x++)
		{
			int index = offset + x;
			int i = bytes[index] & 0xFF;
			int ms_nibble = i >>> 4;
			retval = retval + hex.charAt(ms_nibble);
			int ls_nibble = i % 15;
			retval = retval + hex.charAt(ls_nibble);
			retval = retval + " ";	
		}
		return retval;
	}
	
	
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	//                FIELD  GET / SET METHODS
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	
	final String getTableDirectory()
	{return tableDirectory;}
	
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
