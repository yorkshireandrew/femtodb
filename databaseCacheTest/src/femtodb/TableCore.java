package femtodb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import femtodbexceptions.FemtoDBConcurrentModificationException;
import femtodbexceptions.FemtoDBIOException;
import femtodbexceptions.FemtoDBInvalidValueException;
import femtodbexceptions.FemtoDBPrimaryKeyNotFoundException;
import femtodbexceptions.FemtoDBPrimaryKeyUsedException;
import femtodbexceptions.FemtoDBShuttingDownException;
import femtodbexceptions.FemtoDBTableDeletedException;

public class TableCore implements Serializable, Lock{
	private static final long serialVersionUID = 1L;
	
	static final int 			DEFAULT_FILE_SIZE_IN_BYTES				= 3000;
	static final int 			DEFAULT_CACHE_SIZE_IN_BYTES				= 1000000;
	static final double			DEFAULT_REMOVE_OCCUPANCY_RATIO			= 0.2;
	static final double 		DEFAULT_ALLOW_COMBINE_OCCUPANCY_RATIO  	= 0.9;
	static final long			NOT_MODIFIED_LRU_BOOST					= 10;
	static final long			OVER_HALF_FULL_LRU_BOOST				= 5;
	static final long  			PK_CACHE_NOT_SET 						= Long.MAX_VALUE;
	static final short 			FLAG_CACHE_NOT_SET 						= Short.MIN_VALUE;
	
	static final int			ROW_WRITELOCK							= 0x8000;
	static final int			ROW_READLOCK							= 0x4000;

	/** The database that contains this tableCore */
	private transient FemtoDB	database;
	
	/** The name of the tableCore, shown in exceptions */
	private final String 		name;
	
	/** The tableCore number */
	private long				tableNumber;
	
	/** The directory the tables files will be put in. Declared transient so it adapts to operating system seperators */
	private transient String	tableDirectory;
	
	/** The size of the storage files in bytes */
	private int					fileSize;
	
	/** The number of rows in each file */
	private int					rowsPerFile;
	
	/** Was rowsPerFile set manually */
	private boolean				rowsPerFileSet;
	
	/** If a file's occupancy ratio is below this value, Should it gets removed
	 * from the cache then the tableCore will attempt to combine it into neighbouring files */
	private double				removeOccupancyRatio;
	
	/** The actual number of occupied rows below which combination into neighbouring files is triggered */
	private int					removeOccupancy;
	
	/** The maximum occupancy ratio a file is permitted to have after a neighbouring file has being combined with it.
	 * This should be less than one to reduce combination-split thrashing */
	private double				combineOccupancyRatio;
	
	/** The actual number of occupied rows a file is permitted to have following a combination with a neighbour */
	private int					combineOccupancy;
	
	/** Has the tableCore been made operational */
	private boolean				operational;
	
	/** The next free file number, so the naming of each file is unique */
	private long				nextUnusedFileNumber;
	
	// ************ COLUMN INFORMATION *******************
		
	/** Arrays and values for column meta data */
	private String[]			columnNames;
			int[]				columnByteOffset;
			int[]				columnByteWidth;
	private int					tableWidth;
	
	// ************ CACHES AND META DATA TABLES **********
	/** The cache size in bytes. */
	private int							cacheSize;
	
	/** Was the target cache size set manually */
	private boolean						cacheSizeSet;
	
	/** The number of pages (files) held in the cache */
	private int 						cachePages;
	
	
	/** The cache for the tableCore. Not serialised and must be reallocated on loading */
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
	private transient FileMetadata[]	cacheContents;	
			
	/** The meta data on all the tables files, holding what is in each file and its cache status */
	private List<FileMetadata>			fileMetadata;
	
	/** Count used for LRU caching and file version marking */
	private long						serviceNumber;
	
	// ***************** RowAccessTypeFactory ***************************
	private boolean rowAccessTypeFactorySet = false;
	private RowAccessTypeFactory rowAccessTypeFactory;
	
	/** Indicates the table has been deleted */
	private boolean							deleted;
	
	/** Indicates the table is shutting down */
	private boolean							shuttingDown;
	
	/** Locking */
	private transient ReentrantLock			tableLock;
	private boolean							shutdownNeedsLock;
	private boolean							backupNeedsLock;
		
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	//                   Constructor
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************	

	TableCore(final FemtoDB database, final String name, final long tableNumber, String primaryKeyName)
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
		operational 		= false;
		
		deleted				= false;
		shuttingDown		= false;
		
		removeOccupancyRatio 	= DEFAULT_REMOVE_OCCUPANCY_RATIO;
		combineOccupancyRatio 	= DEFAULT_ALLOW_COMBINE_OCCUPANCY_RATIO;
		
		// add primary key and status columns
		if(primaryKeyName == null)primaryKeyName="femtodb_primary_key";
		addLongColumn(primaryKeyName);
		addShortColumn("femtodb_status");
		
		rowAccessTypeFactorySet = false;
		
		// set service number to a very low number, but not too low for the LRU algorithm
		serviceNumber 			= Long.MIN_VALUE + NOT_MODIFIED_LRU_BOOST + OVER_HALF_FULL_LRU_BOOST + 100;

		// Table lock
		tableLock 				= new ReentrantLock(true);
		backupNeedsLock 		= false;
		shutdownNeedsLock 		= false;
	}

	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	//                COLUMN ADDING METHODS
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	
	/** Adds a byte column to the tableCore */
	public final void addByteColumn(String columnName)
	{
		if(operational) 	return;
		columnNames 		= addToArray(columnNames, columnName);
		columnByteOffset 	= addToArray(columnByteOffset,tableWidth);
		columnByteWidth 	= addToArray(columnByteWidth,1);
		tableWidth++;
	}
	
	/** Adds a Boolean column to the tableCore, its true width is one byte */
	public final void addBooleanColumn(final String columnName)
	{
		if(operational) 	return;
		columnNames 		= addToArray(columnNames, columnName);
		columnByteOffset 	= addToArray(columnByteOffset,tableWidth);
		columnByteWidth 	= addToArray(columnByteWidth,1);
		tableWidth++;
	}
	
	/** Adds a byte array column to the tableCore, its true width is width + 4 bytes to encode length*/
	public final void addByteArrayColumn(final String columnName, final int width) 
	{
		if(operational) 	return;
		columnNames 		= addToArray(columnNames, columnName);
		columnByteOffset 	= addToArray(columnByteOffset,tableWidth);
		int trueWidth 		= 4 + width;
		columnByteWidth 	= addToArray(columnByteWidth,trueWidth);
		tableWidth 			+= trueWidth;
	}
	
	/** Adds a Short column to the tableCore, its true width is 2 bytes */
	public final void addShortColumn(final String columnName)
	{
		if(operational)		return;
		columnNames 		= addToArray(columnNames, columnName);
		columnByteOffset 	= addToArray(columnByteOffset,tableWidth);
		columnByteWidth 	= addToArray(columnByteWidth,2);
		tableWidth 			+= 2;
	}
	
	/** Adds a Character column to the tableCore, its true width is 2 bytes */
	public final void addCharColumn(final String columnName)
	{
		if(operational)		return;
		columnNames 		= addToArray(columnNames, columnName);
		columnByteOffset 	= addToArray(columnByteOffset,tableWidth);
		columnByteWidth 	= addToArray(columnByteWidth,2);
		tableWidth 			+= 2;
	}
	
	/** Adds a Integer column to the tableCore, its true width is 2 bytes */
	public final void addIntegerColumn(final String columnName)
	{
		if(operational)		return;
		columnNames 		= addToArray(columnNames, columnName);
		columnByteOffset 	= addToArray(columnByteOffset,tableWidth);
		columnByteWidth 	= addToArray(columnByteWidth,4);
		tableWidth 			+= 4;
	}
	
	/** Adds a Long column to the tableCore, its true width is 2 bytes */
	public final void addLongColumn(final String columnName)
	{
		if(operational) 	return;
		columnNames 		= addToArray(columnNames, columnName);
		columnByteOffset 	= addToArray(columnByteOffset,tableWidth);
		columnByteWidth 	= addToArray(columnByteWidth,8);
		tableWidth 			+= 8;
	}
	
	/** Adds a Float column to the tableCore, its true width is 2 bytes */
	public final void addFloatColumn(final String columnName)
	{
		if(operational) 	return;
		columnNames 		= addToArray(columnNames, columnName);
		columnByteOffset 	= addToArray(columnByteOffset,tableWidth);
		columnByteWidth 	= addToArray(columnByteWidth,4);
		tableWidth 			+= 4;
	}
	
	/** Adds a Double column to the tableCore, its true width is 2 bytes */
	public final void addDoubleColumn(final String columnName) 
	{
		if(operational) 	return;
		columnNames 		= addToArray(columnNames, columnName);
		columnByteOffset 	= addToArray(columnByteOffset,tableWidth);
		columnByteWidth 	= addToArray(columnByteWidth,8);
		tableWidth 			+= 8;
	}
	
	/** Adds a Char array column to the tableCore, its true width is width*2 + 2 bytes to encode length*/
	public final void addCharArrayColumn(final String columnName, final int width)
	{
		if(operational) 	return;
		columnNames 		= addToArray(columnNames, columnName);
		columnByteOffset 	= addToArray(columnByteOffset,tableWidth);
		int trueWidth 		= 2 + width * 2;
		columnByteWidth 	= addToArray(columnByteWidth,trueWidth);
		tableWidth 			+= trueWidth;
	}
	
	/** Adds a String column to the tableCore, its true width is width + 2 bytes, to encode the length. It is important to note the string gets stored in modified UTF format so the available width in characters may be less than the width parameter */
	public final void addStringColumn(final String columnName, final int width) 
	{
		if(operational) 	return;
		columnNames 		= addToArray(columnNames, columnName);
		columnByteOffset 	= addToArray(columnByteOffset,tableWidth);
		int trueWidth 		= 2 + width;
		columnByteWidth 	= addToArray(columnByteWidth,trueWidth);
		tableWidth 			+= trueWidth;
	}
	
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	//                   TABLE MAKE OPERATIONAL METHODS
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************	

	final void setRowsPerFile(final int rows)
	{
		if(operational) return;
		rowsPerFile = rows;
		rowsPerFileSet = true;
	}
	
	public final void setCacheSize(final int cacheSize)
	{
		if(operational) return;
		this.cacheSize = cacheSize;
		cacheSizeSet = true;
	}
	
	
	// *********************************************
	//          MAKE OPERATIONAL
	// *********************************************
	/** Allocates memory and creates first file making the tableCore operational */
	public final void makeOperational()throws FemtoDBInvalidValueException, FemtoDBIOException
	{
		if(operational) return;
		operational = true;
		
		long actualFileSize;
		
		// Validate rowsPerFile if set otherwise set it automatically to a good value 
		if(rowsPerFileSet)
		{
			if(rowsPerFile <= 0) throw new FemtoDBInvalidValueException("TableCore " + name + " Files must contain at least one row");
			actualFileSize = rowsPerFile;
			actualFileSize = actualFileSize * tableWidth;
			if(actualFileSize > Integer.MAX_VALUE) throw new FemtoDBInvalidValueException("TableCore " + name + " Rows per file multiplied tableCore width exceeds an integer value");
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
		if(removeOccupancyRatio >= 0.5) throw new FemtoDBInvalidValueException("TableCore " + name + " removeOccupancyRatio must be set less than 0.5. It was " + removeOccupancyRatio);
		removeOccupancy = (int)(rowsPerFile * removeOccupancyRatio);
		
		// Validate and set combine occupancy
		if(combineOccupancyRatio > 1) throw new FemtoDBInvalidValueException("TableCore " + name + " combineOccupancyRatio cannot exceed one. It was " + combineOccupancyRatio);
		combineOccupancy = (int)(rowsPerFile * combineOccupancyRatio);
		
		if(combineOccupancy <= removeOccupancy) throw new FemtoDBInvalidValueException("TableCore " + name + " Resulting removeOccupancy must be less than combineOccupancy for file combining to function correctly. It was " + removeOccupancy + ":" + combineOccupancy);

		// Validate cache size if set otherwise set it automatically to the default value
		if(cacheSizeSet)
		{
			if(cacheSize < actualFileSize)throw new FemtoDBInvalidValueException("TableCore " + name + " the cache size must be at least the actual file size " + actualFileSize);
		}
		else
		{
			cacheSize = DEFAULT_CACHE_SIZE_IN_BYTES;
		}
		
		// set the fileSize, cachePages and cacheSize to their final values
		fileSize 	= (int)actualFileSize;
		cachePages 	= cacheSize / (int)actualFileSize;
		cacheSize 	= cachePages * (int)actualFileSize;
		
		allocateMemory();
		
		// Initialise nextFileNumber for creating unique filenames
		nextUnusedFileNumber = 0;
		
		// Create the directory
		tableDirectory = database.getPath() + File.separator + Long.toString(tableNumber);
		File f = new File(tableDirectory);
		if(f.exists()){FileUtils.recursiveDelete(f);}
		if(!f.mkdir())
		{
			throw new FemtoDBIOException("TableCore " + name + " was unable to create directory " + tableDirectory);
		}
		
		// Fill the fileMetadata with a single entry referring to an empty file 
		fileMetadata = new ArrayList<FileMetadata>();
		FileMetadata firstFile = new FileMetadata(
				this,
				nextFilenumber(), 
				Long.MIN_VALUE, // ensure the first primary key added falls into this file
				Long.MAX_VALUE,
				Long.MIN_VALUE,	// ensure first value in tableCore inserts after the zero rows
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
		// the first entry gets inserted in the tableCore.
		freeCachePage(0);
		
		// create the RowAccessTypeFactory
		if(!rowAccessTypeFactorySet)
		{
			rowAccessTypeFactory = new DefaultRowAccessTypeFactory(tableWidth);
		}
	}	
	
	private  final void allocateMemory()
	{
		// allocate memory for the cache
		try{
			cache = new byte[cacheSize];
		}catch(OutOfMemoryError e)
		{
			// re-throw any memory exception providing more information
			throw new OutOfMemoryError("TableCore " + name + " was unable to allocate its cache of " + cacheSize + " bytes");
		}
		
		// allocate memory for the pkCache
		try{
			int maxRowsInCache = rowsPerFile * cachePages;
			pkCache = new long[(maxRowsInCache)];
			for(int x = 0; x < maxRowsInCache;x++){pkCache[x] = PK_CACHE_NOT_SET;}
		}catch(OutOfMemoryError e)
		{
			// re-throw any memory exception providing more information
			throw new OutOfMemoryError("TableCore " + name + " was unable to allocate its primary key cache");
		}
		
		// allocate memory for the pkCacheEraser
		try{
			pkCacheEraser = new long[rowsPerFile];
			for(int x = 0; x < rowsPerFile;x++){pkCacheEraser[x] = PK_CACHE_NOT_SET;}
		}catch(OutOfMemoryError e)
		{
			// re-throw any memory exception providing more information
			throw new OutOfMemoryError("TableCore " + name + " was unable to allocate its primary key cache eraser");
		}

		// allocate memory for the flagCache
		try{
			int maxRowsInCache = rowsPerFile * cachePages;
			flagCache = new short[(maxRowsInCache)];
			for(int x = 0; x < maxRowsInCache;x++){pkCache[x] = FLAG_CACHE_NOT_SET;}
		}catch(OutOfMemoryError e)
		{
			// re-throw any memory exception providing more information
			throw new OutOfMemoryError("TableCore " + name + " was unable to allocate its flag cache");
		}
		
		// allocate memory for the flagCacheEraser
		try{
			flagCacheEraser = new short[rowsPerFile];
			for(int x = 0; x < rowsPerFile;x++){flagCacheEraser[x] = FLAG_CACHE_NOT_SET;}
		}catch(OutOfMemoryError e)
		{
			// re-throw any memory exception providing more information
			throw new OutOfMemoryError("TableCore " + name + " was unable to allocate its flag cache eraser");
		}
		
		// set the cache page contents
		try{
			// Do not need to fill as null indicates the cache page is free
			cacheContents = new FileMetadata[cachePages];
		}catch(OutOfMemoryError e)
		{
			// re-throw any memory exception providing more information
			throw new OutOfMemoryError("TableCore " + name + " was unable to allocate its cache contents array");
		}			
	}	
	
	//******************************************************
	//******************************************************
	//      START OF LOADING INTO AND FREEING CACHE CODE
	//******************************************************
	//******************************************************	
	
	 /**
	 * The main function used by the tableCore for fetching a required file into the cache. It finds the LRU page in the cache, frees it, then uses that page to load the requested file.
	 * @param toLoad		A FileMetadata object indicating which file to load into the cache.
	 * @return 				The cache page the file was loaded into.
	 * @throws IOException	Thrown if there was a problem flushing the LRU cache page, or loading the requested file.
	 */
	private final int loadFileIntoCache(final FileMetadata toLoad) throws FemtoDBIOException
	{		
		// free a different page in the cache without attempting to combine
		int pageToForceFree = findLRUCachePage();
		freeCachePage(pageToForceFree);
	
		// cache the file associated with toSave
		loadFileIntoCachePage(pageToForceFree, toLoad);
		return pageToForceFree;
	}
	
	/** Fills a given cache page reading the file referenced by its FileMetadata fmd argument from disk. The page must be made free before this method is called. */
	private final void loadFileIntoCachePage(final int page, final FileMetadata fmd) throws FemtoDBIOException
	{
		// load the file
		File f = new File(fmd.filename);
		try
		{
			FileInputStream fis = new FileInputStream(f);
			int bytesToRead = tableWidth * fmd.rows;
			int readByteCount = fis.read(cache, (page * fileSize), bytesToRead);
			if(readByteCount != bytesToRead) throw new IOException("TableCore " + name + "(" + tableNumber + ") Read incorrect number of bytes from file " + fmd.filename + " expected " + bytesToRead + " and read " + readByteCount);
			fis.close();
		}
		catch(IOException e){throw new FemtoDBIOException(e.getMessage(),e);}
		
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
	private final int loadFileIntoCacheIgnoringGivenCachePage(final FileMetadata toLoad, final int pageToExclude) throws FemtoDBIOException
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
	private final long cacheLRURankingAlgorithm(final int page, final int halfOfRowsPerFile)
	{
		FileMetadata fmd = cacheContents[page];
		if(fmd == null)return Long.MIN_VALUE; // perfect an unused page :-)
		long retval = fmd.lastUsedServiceNumber;
		if(fmd.modified) retval -= NOT_MODIFIED_LRU_BOOST;
		if(fmd.rows > halfOfRowsPerFile)retval -= OVER_HALF_FULL_LRU_BOOST;
		return retval;
	}
	
	/** Free's a given cache page writing its contents to disk. */
	private final void freeCachePage(final int page) throws FemtoDBIOException
	{	
		flushCachePage(page);
		FileMetadata fmd = cacheContents[page];
		if(fmd == null)return; // cache page must already be free
		
		// mark the fmd as not cached and the cachePageContents as now free
		fmd.cached = false;
		fmd.cacheIndex = -1;
		cacheContents[page] = null;
	}
	
	/** Writes a cache page to disk. */
	private final void flushCachePage(final int page) throws FemtoDBIOException
	{	
		FileMetadata fmd = cacheContents[page];
//		System.out.println("freeing cache page " + page);
		if(fmd == null)return; // cache page must already be free
		if(fmd.modified)
		{
//			System.out.println("containing " + fmd.toString());
			File f = new File(fmd.filename);
			try
			{
				FileOutputStream fos = new FileOutputStream(f);
				fos.write(cache, (page * fileSize), (tableWidth * fmd.rows));
				fos.flush();
				fos.close();
				
				fmd.modified = false; // disk now matches cache
			}
			catch(IOException e){throw new FemtoDBIOException(e.getMessage(),e);}
		}
	}

	//******************************************************
	//******************************************************
	//         START OF COMBINE CODE
	//******************************************************
	//******************************************************	
	
	/** Attempts to combine the file related to a given cache page into its neighbours, freeing the cache page*/
	private final void tryToCombine(final int page, final FileMetadata cacheToFreeFMD) throws FemtoDBIOException
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
			System.out.println("combining with front");
			combineStage2(page,cacheToFreeFMD,frontFMD,true);
		}
		else
		{
			System.out.println("combining with front");
			combineStage2(page,cacheToFreeFMD,backFMD,false);
		}	
	}
	
	/** Combines a given cached file, on a given cache page, into one of its neighbours */
	private final void combineStage2(final int page, final FileMetadata toCombineFMD, FileMetadata targetFMD, boolean isFront) throws FemtoDBIOException
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
		if(toCombineFMDLastUsedServiceNumber > targetFMD.lastUsedServiceNumber)targetFMD.lastUsedServiceNumber = toCombineFMDLastUsedServiceNumber;	
		targetFMD.modified = true;

		// free up toCombine cache and remove file
		cacheContents[page] = null;
		File f = new File(toCombineFMD.filename);
		f.delete();
		fileMetadata.remove(toCombineFMD);		
	}
	
	private final void combineWithFront(final int page, final FileMetadata toCombineFMD, final FileMetadata frontFMD)
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
	
	private final void combineWithBack(final int page, final FileMetadata toCombineFMD, final FileMetadata backFMD)
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
	//         START OF SPLITTING CODE
	//******************************************************
	//******************************************************	

	/** Called when a page becomes full. It is split in half generating a new file and fileMetadata entry. */
	private final void splitFile(final int page, final FileMetadata fmd) throws FemtoDBIOException
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
		try{
			FileOutputStream fos = new FileOutputStream(f);
			fos.write(cache, (page * fileSize) + (newRowsInFirst * tableWidth), (newRowsInSecond * tableWidth));
			fos.flush();
			fos.close();	
		}
		catch(IOException e){throw new FemtoDBIOException(e.getMessage(),e);}

	}

	//******************************************************
	//******************************************************
	//         START OF INSERT CODE
	//******************************************************
	//******************************************************
	
	/** Inserts a row with a given primary key into the tableCore, throws a PrimaryKeyUsedException if the primary key already exists. 
	 * @throws FemtoDBTableDeletedException 
	 * @throws FemtoDBShuttingDownException */
	public final void insert(final long primaryKey, final RowAccessType toInsert) throws FemtoDBIOException, FemtoDBPrimaryKeyUsedException, FemtoDBShuttingDownException, FemtoDBTableDeletedException
	{
		toInsert.prepareFlagsForPersisting();
		boolean inserted = insertCore(primaryKey, toInsert.flags, toInsert.byteArray);
		if(!inserted)throw new FemtoDBPrimaryKeyUsedException("Primary key " + primaryKey +" already in tableCore " + name);
	}
	
	/** Inserts a row with a given primary key into the tableCore or does nothing (and also returns false) if the primary key already exists. 
	 * @throws FemtoDBTableDeletedException 
	 * @throws FemtoDBShuttingDownException */
	final boolean insertOrIgnore(final long primaryKey, final RowAccessType toInsert) throws FemtoDBIOException, FemtoDBShuttingDownException, FemtoDBTableDeletedException
	{
		return insertCore(primaryKey, toInsert.flags, toInsert.byteArray);
	}
	
	final boolean insertOrIgnoreByteArrayByPrimaryKey(final long primaryKey, final byte[] toInsert) throws FemtoDBIOException, FemtoDBShuttingDownException, FemtoDBTableDeletedException
	{
			return insertCore(primaryKey, FLAG_CACHE_NOT_SET, toInsert);
	}
	
	/**
	 * 
	 * @param primaryKey			The primary key value where the row data will be inserted.
	 * @param flag					What to place in the flag cache, use FLAG_CACHE_NOT_SET if this value is not known.
	 * @param toInsert				The byte array representing the row.
	 * @return						True if an insert occurred
	 * @throws FemtoDBIOException
	 * @throws FemtoDBShuttingDownException 
	 * @throws FemtoDBTableDeletedException 
	 */
	synchronized
	private final boolean insertCore(final long primaryKey, final short flag, final byte[] toInsert) throws FemtoDBIOException, FemtoDBShuttingDownException, FemtoDBTableDeletedException
	{
		if(shuttingDown)throw new FemtoDBShuttingDownException();
		if(deleted)throw new FemtoDBTableDeletedException();
		
		serviceNumber++;
		int fileMetadataListIndex 	= fileMetadataBinarySearch(primaryKey);		
		FileMetadata fmd 			= fileMetadata.get(fileMetadataListIndex);
		int fmdRows 				= fmd.rows;

		// Ensure the file containing the range the primary key falls in is loaded into the cache
		int page = cachePageOf(fmd);
		
		// handle the special case of an empty tableCore
		if(fmdRows == 0)
		{
			insertIntoEmptyPage(primaryKey, toInsert, page, fmd);
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
			if(insertRow == -1)
			{
				return false; // primary key already in use
			}
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
		int desPos1 				= srcPos1 + 1;
		System.arraycopy(pkCacheL, srcPos1, pkCacheL, desPos1, (fmdRows - insertRow) );
		System.arraycopy(flagCacheL, srcPos1, flagCacheL, desPos1, (fmdRows - insertRow) );
		
		// make room for the insert into the cache
		int srcPos2 				= cachePageStart + insertRow * tableWidthL;
		System.arraycopy(cacheL, srcPos2, cacheL, (srcPos2 + tableWidthL), (fmdRows - insertRow) * tableWidthL);

		// perform the insert
		pkCacheL[srcPos1] 			= primaryKey;
		flagCacheL[srcPos1]			= flag;
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
	
	private final void insertIntoEmptyPage(final long primaryKey, final byte[] toInsert, final int page, final FileMetadata fmd)
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
	//         START OF UPDATE CODE
	//******************************************************
	//******************************************************
	
	/** Updates a row given a primary key, throwing an exception if it is not present.
	 * @param primaryKey			The primary key value where the row data will be inserted.
	 * @param theNewRow				The RowAccessType representing the row to be placed into the table
	 * @throws FemtoDBIOException
	 * @throws FemtoDBShuttingDownException 
	 * @throws FemtoDBTableDeletedException 
	 * @throws FemtoDBPrimaryKeyNotFoundException
	 */
	public final void update(final long primaryKey, RowAccessType theNewRow) throws FemtoDBIOException, FemtoDBShuttingDownException, FemtoDBTableDeletedException, FemtoDBPrimaryKeyNotFoundException
	{
		theNewRow.prepareFlagsForPersisting();
		boolean updated = updateOrIgnore(primaryKey, theNewRow.flags, theNewRow.byteArray);
		if(!updated)throw new FemtoDBPrimaryKeyNotFoundException("The primary key " + primaryKey + " was not found when updating a row in tableCore " + name);		
	}

	 
	/** Updates a row given a primary key.
	* @param primaryKey			The primary key value where the row data will be inserted.
	* @param theNewRow				The RowAccessType representing the row to be placed into the table
	* @return						True if an update occurred (the primary key exists)
	* @throws FemtoDBIOException
	* @throws FemtoDBShuttingDownException 
	* @throws FemtoDBTableDeletedException 
	*/	 
	public final boolean updateOrIgnore(final long primaryKey, RowAccessType theNewRow) throws FemtoDBIOException, FemtoDBShuttingDownException, FemtoDBTableDeletedException
	{
		theNewRow.prepareFlagsForPersisting();
		return updateOrIgnore(primaryKey, theNewRow.flags, theNewRow.byteArray);
	}	
	
	final void update(final long primaryKey, final short flag, final byte[] toInsert) throws FemtoDBIOException, FemtoDBPrimaryKeyNotFoundException, FemtoDBShuttingDownException, FemtoDBTableDeletedException
	{
		boolean updated = updateOrIgnore(primaryKey, flag, toInsert);
		if(!updated)throw new FemtoDBPrimaryKeyNotFoundException("The primary key " + primaryKey + " was not found when updating a row in tableCore " + name);
	}
	
	/** Updates a row given a primary key.
	 * 
	 * @param primaryKey			The primary key value where the row data will be inserted.
	 * @param flag					What to place in the flag cache, use FLAG_CACHE_NOT_SET if this value is not known.
	 * @param toUpdate				The byte array representing the row.
	 * @return						True if an update occurred (the primary key exists)
	 * @throws FemtoDBIOException
	 * @throws FemtoDBShuttingDownException 
	 * @throws FemtoDBTableDeletedException 
	 */
	synchronized
	final boolean updateOrIgnore(final long primaryKey, final short flag, final byte[] toUpdate) throws FemtoDBIOException, FemtoDBShuttingDownException, FemtoDBTableDeletedException
	{
		if(shuttingDown)throw new FemtoDBShuttingDownException();
		if(deleted)throw new FemtoDBTableDeletedException();
		serviceNumber++;
		int fileMetadataListIndex 	= fileMetadataBinarySearch(primaryKey);		
		FileMetadata fmd 			= fileMetadata.get(fileMetadataListIndex);

		// Ensure the file containing the range the primary key falls in is loaded into the cache
		int page = cachePageOf(fmd);
		
		// Find the update point
		int updateRow = primaryKeyBinarySearch(page, primaryKey, false,false);		
		if(updateRow == -1)
		{
			return false; // primary key does not exist
		}
		
		// localise class fields for speed
		int 	tableWidthL 		= tableWidth;
			
		// calculate indexes
		int 	srcPos1 			= page * rowsPerFile + updateRow;		
		int 	srcPos2 			= page * fileSize + updateRow * tableWidthL;

		// perform the insert
		pkCache[srcPos1] 			= primaryKey;
		flagCache[srcPos1]			= flag;
		System.arraycopy(toUpdate, 0, cache, srcPos2, tableWidthL);
		
		// update file meta data
		fmd.lastUsedServiceNumber 		= serviceNumber;
		fmd.modificationServiceNumber 	= serviceNumber;
		fmd.modified					= true;	
		return true;
	}
	
	/** Checks the read lock on a row in the database tableCore, returns one if locked, zero if unlocked and -1 if the primary key does not exist 
	 * @throws FemtoDBShuttingDownException 
	 * @throws FemtoDBTableDeletedException */
	synchronized
	public final int checkRowReadLock(final long primaryKey) throws FemtoDBIOException, FemtoDBShuttingDownException, FemtoDBTableDeletedException
	{
		if(shuttingDown)throw new FemtoDBShuttingDownException();
		if(deleted)throw new FemtoDBTableDeletedException();
		serviceNumber++;
		int fileMetadataListIndex 	= fileMetadataBinarySearch(primaryKey);		
		FileMetadata fmd 			= fileMetadata.get(fileMetadataListIndex);

		// Ensure the file containing the range the primary key falls in is loaded into the cache
		int page = cachePageOf(fmd);
		
		// Find the update point
		int rowToCheck = primaryKeyBinarySearch(page, primaryKey, false,false);		
		if(rowToCheck == -1)
		{
			return -1; // primary key does not exist
		}
		
		return (isRowReadLockedLowLevel(page,rowToCheck) ? 1 : 0);
	}
	
	/** Checks the read lock on a row in the database tableCore, returns one if locked, zero if unlocked and -1 if the primary key does not exist 
	 * @throws FemtoDBShuttingDownException 
	 * @throws FemtoDBTableDeletedException */
	synchronized
	public final int checkRowWriteLock(final long primaryKey) throws FemtoDBIOException, FemtoDBShuttingDownException, FemtoDBTableDeletedException
	{
		if(shuttingDown)throw new FemtoDBShuttingDownException();
		if(deleted)throw new FemtoDBTableDeletedException();
		serviceNumber++;
		int fileMetadataListIndex 	= fileMetadataBinarySearch(primaryKey);		
		FileMetadata fmd 			= fileMetadata.get(fileMetadataListIndex);

		// Ensure the file containing the range the primary key falls in is loaded into the cache
		int page = cachePageOf(fmd);
		
		// Find the update point
		int rowToCheck = primaryKeyBinarySearch(page, primaryKey, false,false);		
		if(rowToCheck == -1)
		{
			return -1; // primary key does not exist
		}
		
		return (isRowWriteLockedLowLevel(page,rowToCheck) ? 1 : 0);
	}
	
	final boolean isRowReadLockedLowLevel(final int page, final int row)
	{
		
		int src1 = page * rowsPerFile + row;
		int src2 = page * fileSize + row * tableWidth;
		short tablesCurrentFlags = flagCache[src1];
		if(tablesCurrentFlags == FLAG_CACHE_NOT_SET)
		{
			tablesCurrentFlags = BuffRead.readShort(cache, src2 + 8);
		}
		if((tablesCurrentFlags & ROW_READLOCK) != 0)return true;
		return false;	
	}
	
	final boolean isRowWriteLockedLowLevel(int page, int row)
	{
		int src1 = page * rowsPerFile + row;
		int src2 = page * fileSize + row * tableWidth;
		short tablesCurrentFlags = flagCache[src1];
		if(tablesCurrentFlags == FLAG_CACHE_NOT_SET)
		{
			tablesCurrentFlags = BuffRead.readShort(cache, src2 + 8);
		}
		if((tablesCurrentFlags & ROW_WRITELOCK) != 0)return true;
		return false;	
	}
	
	//******************************************************
	//******************************************************
	//         START OF SEEK CODE
	//******************************************************
	//******************************************************	
	
	/** Returns the underlying byte array representation of a row, given a primary key and serviceNumber. Introduced during the initial testing phase */
	final byte[] seekByteArray(final long primaryKey) throws FemtoDBIOException
	{
		serviceNumber++;
//		System.out.println("seeking " + primaryKey);
		int fileMetadataListIndex = fileMetadataBinarySearch(primaryKey);
//		System.out.println("is in metafile index " + fileMetadataListIndex);
		// ensure the file containing the range the primary key falls in is loaded into the cache
		FileMetadata fmd = fileMetadata.get(fileMetadataListIndex);
		int page = cachePageOf(fmd);
		
		if(fmd.rows == 0)
		{
				System.out.println("empty tableCore!");
				return null;	// empty tableCore!		
		}
		Integer row = primaryKeyBinarySearch(page, primaryKey, false, false);
		if(row == null)
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
	
	/** Returns a RowAccessType given a primary key, or null if it does not exist. Requires a serviceNumber for LRU caching 
	 * @throws FemtoDBShuttingDownException 
	 * @throws FemtoDBTableDeletedException */
	synchronized
	public final RowAccessType seek(final long primaryKey) throws FemtoDBIOException, FemtoDBShuttingDownException, FemtoDBTableDeletedException
	{
		if(shuttingDown)throw new FemtoDBShuttingDownException();
		if(deleted)throw new FemtoDBTableDeletedException();
		serviceNumber++;
//		System.out.println("seeking " + primaryKey);
		int fileMetadataListIndex = fileMetadataBinarySearch(primaryKey);
//		System.out.println("is in metafile index " + fileMetadataListIndex);
		// ensure the file containing the range the primary key falls in is loaded into the cache
		FileMetadata fmd = fileMetadata.get(fileMetadataListIndex);
		
		int page = cachePageOf(fmd);
		
		if(fmd.rows == 0)
		{
				System.out.println("empty tableCore!");
				return null;	// empty tableCore!		
		}
		Integer row = primaryKeyBinarySearch(page, primaryKey, false, false);
		if(row == null)
		{
				System.out.println("primary key not found!");
				return null;		// primary key not found
		}
		
		int flagSrcPos = page * rowsPerFile + row;
		RowAccessType retval = rowAccessTypeFactory.createRowAccessType(primaryKey, flagCache[flagSrcPos], this);	
		int srcPos = page * fileSize + row * tableWidth;	
		System.arraycopy(cache, srcPos, retval.byteArray, 0, tableWidth);
		fmd.lastUsedServiceNumber = serviceNumber;
		return retval;
	}
	
	//******************************************************
	//******************************************************
	//         START OF DELETE ROW CODE
	//******************************************************
	//******************************************************	
	
	/** Deletes a row in the tableCore given its primary key and a serviceNumber for the operation. Returns true if the primary key existed. 
	 * @throws FemtoDBShuttingDownException 
	 * @throws FemtoDBTableDeletedException */
	synchronized
	public final boolean deleteByPrimaryKey(final long primaryKey) throws FemtoDBIOException, FemtoDBShuttingDownException, FemtoDBTableDeletedException
	{
		System.out.println("DELETE BY PRIMARY KEY");
		System.out.println("--------------------------------");
		for(FileMetadata show : fileMetadata)
		{
			System.out.println("" + show.filenumber + " pkmin " + show.smallestPK + " pkmax " + show.largestPK + " rows " + show.rows);
		}
		System.out.println("--------------------------------");
		
		if(shuttingDown)throw new FemtoDBShuttingDownException();
		if(deleted)throw new FemtoDBTableDeletedException();
		serviceNumber++;
		int fileMetadataListIndex = fileMetadataBinarySearch(primaryKey);
		System.out.println("fileMetadataListIndex " + fileMetadataListIndex + " for primary Key " + primaryKey);
		// ensure the file containing the range the primary key falls in is loaded into the cache
		FileMetadata fmd = fileMetadata.get(fileMetadataListIndex);
		
		int page = cachePageOf(fmd);
		System.out.println("check cache page " + page);
		System.out.println(this.cacheToString());
		
		if(fmd.rows == 0)
		{
			System.out.println("EMPTY TABLECORE DURING DELETE");
			return false;	// empty tableCore!		
		}
		int row = primaryKeyBinarySearch(page, primaryKey, false, false);
		if(row == -1)
		{
			System.out.println("FAILED TO FIND PK DURING DELETE");
			return false;		// primary key not found
		}
		System.out.println("row " + row + " for primary Key " + primaryKey);
	
		deleteRow(primaryKey,page,row);
		
		System.out.println(" Now the cache looks like this....");
		System.out.println(this.cacheToString());
		
		System.out.println("--------------------------------");
		for(FileMetadata show : fileMetadata)
		{
			System.out.println("" + show.filenumber + " pkmin " + show.smallestPK + " pkmax " + show.largestPK + " rows " + show.rows);
		}
		System.out.println("--------------------------------");
		
		return true;
	}
	
	/** low level row delete, page must already be in cache */
	private final void deleteRow(final long primaryKey, final int page, final int row) throws FemtoDBIOException
	{
		FileMetadata fmd = cacheContents[page];
		int fmdRows = fmd.rows;
		
		if(fmdRows == 1)
		{
			// special case of emptying tableCore
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
	private final int primaryKeyBinarySearch(final int page, final long primaryKey, final boolean forInsert, final boolean overwritePrevention)
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
				if(forInsert){return testIndex;}
				else return -1;
			}
			priorIndex = testIndex;
			
			testPK 		= getPrimaryKeyForCacheRow(page, testIndex);
//			System.out.println("returned primary key for row " + testPK);
			toBig 	= (primaryKey < testPK);
			toSmall = (primaryKey > testPK);
		}
		
		if(overwritePrevention)
		{
			return -1;
		}
		
		return testIndex;		
	}
	
	//******************************************************
	//******************************************************
	//        START OF ITERATORS
	//******************************************************
	//******************************************************
	/** Returns a FemtoDBIterator for iterating over all the rows of the table. 
	 * It should not be used if the remove method 
	 * is required, or it is known that inserts or deletes may occur on the table whilst
	 * the iterator is in use.
	 * @return The FemtoDBIterator
	 */
	public final FemtoDBIterator fastIterator()
	{
		return (new FemtoDBIterator()
				{
					FileMetadata fmd = null;
					int fmdRows;
					int currentRow;
					
					boolean 		hasNextCalledLast 		= false;
					boolean 		hasNextCalledLastResult = false;
					
					@Override
					public final boolean hasNext() {
						synchronized(TableCore.this)
						{
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
							int hasNextFMDIndex = fileMetadataL.indexOf(hasNextFMD);
							hasNextFMDIndex++;
							int fmdSize 		= fileMetadataL.size();
							while(hasNextFMDIndex < fmdSize)
							{
								hasNextFMD = fileMetadataL.get(hasNextFMDIndex);
								if(hasNextFMD.rows > 0)
								{
									hasNextCalledLast 		= true;
									hasNextCalledLastResult = true;
									return true;
								}
								hasNextFMDIndex++;
							}
							hasNextCalledLast 		= true;
							hasNextCalledLastResult = false;
							return false;
						}
					}

					@Override
					public final RowAccessType next() throws FemtoDBIOException {
						synchronized(TableCore.this)
						{
							serviceNumber++;
							
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
							
							// if we have another row in hasNextFMD return it
							if(currentRow < fmdRows)
							{
								RowAccessType retval = getRowAccessType(fmd, currentRow);
								fmd.lastUsedServiceNumber = serviceNumber;
								return retval;
							}
							
							// Seek forward through fileMetadata list for next row
							int nextFMDIndex = fileMetadataL.indexOf(fmd);
							nextFMDIndex++;
							int fmdSize = fileMetadataL.size();
							while(nextFMDIndex < fmdSize)
							{
								FileMetadata nextFMD = fileMetadataL.get(nextFMDIndex);
								if(nextFMD.rows > 0)
								{
									fmd = nextFMD;
									currentRow = 0;
									fmdRows = nextFMD.rows;
									
									RowAccessType retval = getRowAccessType(nextFMD, 0);
									fmd.lastUsedServiceNumber = serviceNumber;
									return retval;
								}
								nextFMDIndex++;
							}
							return null;
						}
					}

					@Override
					public final void remove() {
						// We don't support remove() because it may alter the fileMetadata tableCore, corrupting the iterator
						throw new UnsupportedOperationException();
					}
					
					@Override
					public final void reset() 
					{
						fmd = null;
						hasNextCalledLast 		= false;
						hasNextCalledLastResult = false;
					}
					
					/** Private method used by Fast Iterator only */
					private final RowAccessType getRowAccessType(final FileMetadata fmd, final int row) throws FemtoDBIOException
					{
						int page = cachePageOf(fmd);
						long primaryKey = getPrimaryKeyForCacheRow(page, row);					
						int flagSrcPos = page * rowsPerFile + row;						
						RowAccessType retval = rowAccessTypeFactory.createRowAccessType(primaryKey, flagCache[flagSrcPos], TableCore.this);
						int srcPos = page * fileSize + row * tableWidth;
						System.arraycopy(cache, srcPos, retval.byteArray, 0, tableWidth);
						return retval;		
					}
				});
		
	}
	
	/** Iterates over all the table rows. 
	 * It implements the remove method and allows insertions and deletions 
	 * either side of the iterators current position.
	 * Deleting the Iterators current position will result in a
	 * FemtoDBConcurrentModificationException being thrown the next time the iterator is used.
	 * @return A FemtoDBIterator to that iterates over all the rows of the table
	 */
	public final FemtoDBIterator safeIterator()
	{
		return (new FemtoDBIterator()
				{
					boolean			hasPrimaryKey = false;
					long 			primaryKey;
					
					@Override
					public final boolean hasNext() throws FemtoDBConcurrentModificationException, FemtoDBIOException{	
						synchronized(TableCore.this)
						{
							List<FileMetadata> fileMetadataL = fileMetadata;
							if(hasPrimaryKey)
							{
								System.out.println("iterator has primary key " + primaryKey);
								int hasNextFMDIndex = fileMetadataBinarySearch(primaryKey);
								if(hasNextFMDIndex == -1)throw new FemtoDBConcurrentModificationException("Primary key " + primaryKey + " disappeared from " + TableCore.this.name + " while iterating");							
								FileMetadata hasNextfmd = fileMetadataL.get(hasNextFMDIndex);
								System.out.println("search gave \n" + hasNextfmd);
								
								// catch case of an empty tableCore (as primaryKeyBinarySearch would return its first row)
								if(hasNextfmd.rows <= 0)throw new FemtoDBConcurrentModificationException("Primary key " + primaryKey + " disappeared from " + TableCore.this.name + " while iterating");
		
								int page = cachePageOf(hasNextfmd);						
								int row = primaryKeyBinarySearch(page, primaryKey, false, false);
								if(row == -1)throw new FemtoDBConcurrentModificationException("Primary key " + primaryKey + " disappeared from " + TableCore.this.name + " while iterating");
								row++; // move forward by one
								if(row < hasNextfmd.rows)
								{
									return true;
								}
								else
								{
									// seek forward through files for next row
									int fmdSize = fileMetadataL.size();
									hasNextFMDIndex++;
									
									while(hasNextFMDIndex < fmdSize)
									{
										FileMetadata fmd = fileMetadataL.get(hasNextFMDIndex);
										if(fmd.rows > 0)return true;
										hasNextFMDIndex++;
									}
									return false; // must be an empty tableCore	
								}			
							}
							else
							{
								// OK we need to find the first primary key
								int fmdIndex 		= 0;
								int fmdSize 		= fileMetadataL.size();
								while(fmdIndex < fmdSize)
								{
									FileMetadata fmd = fileMetadataL.get(fmdIndex);
									if(fmd.rows > 0)return true;
									fmdIndex++;
								}
								return false; // must be an empty tableCore	
							}
						}
					}

					@Override
					public final RowAccessType next() throws FemtoDBIOException, FemtoDBConcurrentModificationException {
						synchronized(TableCore.this)
						{
							serviceNumber++;
							List<FileMetadata> fileMetadataL = fileMetadata;
							if(hasPrimaryKey)
							{
								int nextFMDIndex = fileMetadataBinarySearch(primaryKey);
								if(nextFMDIndex == -1)throw new FemtoDBConcurrentModificationException("Primary key " + primaryKey + " disappeared from " + TableCore.this.name + " while iterating");
								FileMetadata nextFMD = fileMetadataL.get(nextFMDIndex);
								
								// catch case of an empty tableCore (as primaryKeyBinarySearch would return its first row)
								if(nextFMD.rows <= 0)throw new FemtoDBConcurrentModificationException("Primary key " + primaryKey + " disappeared from " + TableCore.this.name + " while iterating");						
								int page = cachePageOf(nextFMD);						
								int row = primaryKeyBinarySearch(page, primaryKey, false, false);
								if(row == -1)throw new FemtoDBConcurrentModificationException("Primary key " + primaryKey + " disappeared from " + TableCore.this.name + " while iterating");
								row++; // move forward by one
								if(row < nextFMD.rows)
								{
									RowAccessType retval = getRowAccessType(nextFMD, row);
									nextFMD.lastUsedServiceNumber = serviceNumber;
									primaryKey = retval.primaryKey;
									hasPrimaryKey = true;
									return retval;								
								}
								else
								{
									// seek forward through files for next row
									int fmdSize = fileMetadataL.size();
									nextFMDIndex++;
									
									while(nextFMDIndex < fmdSize)
									{
										nextFMD = fileMetadataL.get(nextFMDIndex);
										if(nextFMD.rows > 0)
										{
											RowAccessType retval = getRowAccessType(nextFMD, 0);
											nextFMD.lastUsedServiceNumber = serviceNumber;
											primaryKey = retval.primaryKey;
											hasPrimaryKey = true;
											return retval;	
										}
										nextFMDIndex++;
									}
									return null; // must be an empty tableCore	
								}			
							}
							else
							{
								// OK we need to find the first primary key
								int fmdIndex 		= 0;
								int fmdSize 		= fileMetadataL.size();
								while(fmdIndex < fmdSize)
								{
									FileMetadata nextFMD = fileMetadataL.get(fmdIndex);
									if(nextFMD.rows > 0)
									{
										RowAccessType retval = getRowAccessType(nextFMD, 0);
										primaryKey = retval.primaryKey;
										hasPrimaryKey = true;
										nextFMD.lastUsedServiceNumber = serviceNumber;
										return retval;										
									}
									fmdIndex++;
								}
								return null; // must be an empty tableCore	
							}
						}
					}

					@Override
					public final void remove() throws FemtoDBConcurrentModificationException, FemtoDBIOException {
						synchronized(TableCore.this)
						{
							serviceNumber++;
							System.out.println("**** REMOVE CALLED ****");
							System.out.println("looking for primary key " + primaryKey);
							if(!hasPrimaryKey)return;
							List<FileMetadata> fileMetadataL = fileMetadata;
							
							int removeFMDIndex = fileMetadataBinarySearch(primaryKey);
							if(removeFMDIndex == -1)throw new FemtoDBConcurrentModificationException("Primary key " + primaryKey + " disappeared from " + TableCore.this.name + " while iterating");
							FileMetadata removeFMD = fileMetadataL.get(removeFMDIndex);	
							if(removeFMD.rows <= 0)throw new FemtoDBConcurrentModificationException("Primary key " + primaryKey + " disappeared from " + TableCore.this.name + " while iterating");						
							int page = cachePageOf(removeFMD);							
							int row = primaryKeyBinarySearch(page, primaryKey, false, false);
							if(row == -1)throw new FemtoDBConcurrentModificationException("Primary key " + primaryKey + " disappeared from " + TableCore.this.name + " while iterating");
							System.out.println("believe it is in row " + row + "of");
							if(row > 0)
							{
								System.out.println("safeIterator deleting row " + row);
								deleteRow(primaryKey, page, row);
								row--;
								System.out.println("now looing up row " + row);
								primaryKey = getPrimaryKeyForCacheRow(page, row);
								System.out.println("which gave primary key " + primaryKey);
								return;
							}
							else
							{
								System.out.println("safeIterator deleting first row");
								deleteRow(primaryKey, page, row);
								removeFMDIndex--;
								while(removeFMDIndex >= 0)
								{
									removeFMD = fileMetadataL.get(removeFMDIndex);
									System.out.println("looking for row in " + removeFMD.filename);								
									if(removeFMD.rows > 0)
									{
										System.out.println("it has " + removeFMD.rows + " rows");	
										row = removeFMD.rows - 1;
										primaryKey = getPrimaryKeyForCacheRow(page, row);
										System.out.println("last one has primary key " + primaryKey);
										return;		
									}
									removeFMDIndex--;
								}
								// empty tableCore so reset iterator
								System.out.println("empty tableCore so reset the iterator");
								reset();
								return;
							}
						}
					}
					
					@Override
					public void reset() 
					{
						hasPrimaryKey = false;
						primaryKey = -1;
					}
					
					/** Private method used by the Iterator only */
					private final RowAccessType getRowAccessType(final FileMetadata fmd, final int row) throws FemtoDBIOException
					{
						int page = cachePageOf(fmd);
						long primaryKey = getPrimaryKeyForCacheRow(page, row);
						int flagSrcPos = page * rowsPerFile + row;						
						RowAccessType retval = rowAccessTypeFactory.createRowAccessType(primaryKey, flagCache[flagSrcPos],TableCore.this);
						int srcPos = page * fileSize + row * tableWidth;
						System.arraycopy(cache, srcPos, retval.byteArray, 0, tableWidth);
						return retval;		
					}
				});
		
	}
	
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	//                SEVERAL PRIVATE METHODS
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************

	/** Ensures a file is in the cache returning its cache page */
	private final int cachePageOf(final FileMetadata fmd) throws FemtoDBIOException
	{
		if(fmd.cached)
		{
			return fmd.cacheIndex;
		}
		else
		{
//			System.out.println("fmd " + fmd.filename + " says it isnt cached so loading");
//			System.out.println("fmd " + fmd.hashCode());
			return loadFileIntoCache(fmd);
		}
	}
	
	private long getPrimaryKeyForCacheRow(final int page, final int row)
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
	private  final int[] addToArray(final int[] in, final int toAdd)
	{
		int len = in.length;
		int[] retval = Arrays.copyOf(in, len+1);
		retval[len] = toAdd;
		return retval;
	}

	/** Returns a new String array consisting of the passed in array  appended with the passed in value */
	private  final String[] addToArray(final String[] in, final String toAdd)
	{
		int len = in.length;
		String[] retval = Arrays.copyOf(in, len+1);
		retval[len] = toAdd;
		return retval;
	}
	
	/** Returns the next available file number for the tableCore that has never been used */
	private long nextFilenumber() {
		return nextUnusedFileNumber++;
	}
		
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	//         SERIALISATION METHODS
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************

	final void shutdownTable() throws FemtoDBIOException
	{
		boolean requiresLockForShutdownL = shutdownNeedsLock;
		if(requiresLockForShutdownL)tableLock.lock();
		try
		{
			shutdownTableInternal();
		}
		finally
		{
			if(requiresLockForShutdownL)tableLock.unlock();
		}	
	}
	
	synchronized
	private final void shutdownTableInternal() throws FemtoDBIOException
	{
		shuttingDown = true;
		
		String destDirectory = this.database.getPath();
		if(destDirectory != null)
		{
			generateTableFile(this,destDirectory);
		}
		
		if(operational)
		{
			flushCache();			
		}
	}
	
	final void flushCache() throws FemtoDBIOException
    {
    	if(!operational)return;
    	for(int page = 0; page < cachePages; page++)
    	{
    		flushCachePage(page);
    	}	
    }
	
	synchronized
    final void finishLoading(final FemtoDB database)
    {
    	this.database = database;
		tableDirectory = database.getPath() + File.separator + Long.toString(tableNumber);
		this.shuttingDown = false;
		this.tableLock = new ReentrantLock(true);
		
		if(operational)
    	{
    		allocateMemory();
    		for(int x = 0; x < fileMetadata.size(); x++)
    		{
    			fileMetadata.get(x).finishLoading(this);
    		}
    	}	
    }
    
	final void backupCompletely(final String destDirectory) throws FemtoDBIOException
	{
		boolean requiresLockForBackupL = backupNeedsLock;
		if(requiresLockForBackupL)tableLock.lock();
		try{
			 backupCompletelyInternal(destDirectory);
		}
		finally
		{
			if(requiresLockForBackupL)tableLock.unlock();
		}
	}
	
    synchronized
    private final void backupCompletelyInternal(final String destDirectory)  throws FemtoDBIOException
    {  			
		
    	// create or overwrite the table file
		generateTableFile(this,destDirectory);
    	
    	if(!operational)return;
    	
    	// create empty directory to hold the data files if it does not exist
		String tableDirectoryString = destDirectory + File.separator + Long.toString(tableNumber);
		File tableDirectoryFile = new File(tableDirectoryString);
		if(tableDirectoryFile.exists())tableDirectoryFile.delete();
		tableDirectoryFile.mkdirs();
		
		// backup the data files
		flushCache();
	
    	for(FileMetadata fmd : fileMetadata)
    	{
    		File sourceFile = new File(fmd.filename);
    		String fullDestString = tableDirectoryString + File.separator + Long.toString(fmd.filenumber);
    		File destFile = new File(fullDestString);
    		try {
				FileUtils.copyFile(sourceFile, destFile);
			} catch (IOException e) {
				throw new FemtoDBIOException("During backup or save of table " + name + " IOException occured copying table data to file:" + fullDestString,e);
			}
    	}
    }
    
	/** Serialises a given tableCore object into to the directory given by the destString argument. It does not serialise the associated tableCores data files. */
	private final void generateTableFile(final TableCore t, final String destDirectory) throws FemtoDBIOException
	{
		// delete it if it exists
		String tableFileString = destDirectory + File.separator + "tableCore" + t.tableNumber;
		File tableFile = new File(tableFileString);
		if(tableFile.exists())tableFile.delete();
		
		// save the tableCore to the destDirectory
		OutputStream 		table_os = null;
		ObjectOutputStream 	table_oos = null; 
		try {
			table_os = new FileOutputStream(tableFile);
			table_oos = new ObjectOutputStream(table_os);
			table_oos.writeObject(t);		
		} catch (FileNotFoundException e) {
			throw new FemtoDBIOException("The following directory used for backup or save does not exist: " + destDirectory, e);
		} catch (IOException e) {
			throw new FemtoDBIOException("Unable to save or backup table " + name + " an IOException occured creating the following file: " + tableFileString, e);
		} finally
		{
			if(table_oos != null)
			{
				try {
					table_oos.close();
				} catch (IOException e) {
					throw new FemtoDBIOException("Unable to close table file " + name + " during a backup or save at location: " + tableFileString, e);
				}
			}
			if(table_os != null)
			{
				try {
					table_os.close();
				} catch (IOException e) {
					throw new FemtoDBIOException("Unable to close table file " + name + " during a backup or save at location: " + tableFileString, e);
				}
			}		
		}
	}
	
	/** Serialises a given tableCore object into to the directory given by the destString argument. It does not serialise the associated tableCores data files. */
	private final void deleteTableFile(final TableCore t, final String destDirectory) throws FemtoDBIOException
	{
		// delete it if it exists
		String tableFileString = destDirectory + File.separator + "tableCore" + t.tableNumber;
		File tableFile = new File(tableFileString);
		if(tableFile.exists())tableFile.delete();
	}
	
	/** Serialises a given tableCore object into to the directory given by the destString argument. It does not serialise the associated tableCores data files. */
	private final void deleteTableDataDirectory(TableCore t, String destDirectory) throws FemtoDBIOException
	{
		// delete it if it exists
		String dataDirectoryString = destDirectory + File.separator + Long.toString(t.tableNumber);
		File dataDirectory = new File(dataDirectoryString);
		if(dataDirectory.exists())dataDirectory.delete();
	}
	
	final boolean validateTable(final String path)
	{
		String tableDirectoryString = path + File.separator + Long.toString(tableNumber);
    	int tableWidthL = tableWidth;
		for(FileMetadata fmd : fileMetadata)
    	{
    		String fileString = tableDirectoryString + File.separator + Long.toString(fmd.filenumber);
    		File fileToCheck = new File(fileString);
    		if( !fileToCheck.exists() )return false;
    		long fileToCheckSize = fileToCheck.length();
    		long expectedSize = fmd.rows * tableWidthL;
    		if( fileToCheckSize != expectedSize )return false;
    	}
		return true;	
	}
	
	synchronized
	void deleteTable(final String path) throws FemtoDBIOException
	{
		deleted = true;
		deleteTableFile(this, path);
		deleteTableDataDirectory(this, path);
	}
	
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	//         METHODS FOR TEST AND DEBUG
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************
	
	public final String toString()
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
		retval = retval + "nextFileNumber: " 		+ nextUnusedFileNumber + "\n";
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

	public final String cacheToString()
	{
		int cachePagesL 	= cachePages;
		int rowsPerFileL 	= rowsPerFile;
		int fileSizeL 		= fileSize;
		int tableWidthL 	= tableWidth;
		String retval = "";
		for(int page = 0; page < cachePagesL; page ++)
		{
			FileMetadata fxx = cacheContents[page];
			String namey = "null";
			if(fxx != null){namey = fxx.filename;}
			retval = retval + "=========== page " + page + " =================== " + namey + "\n";
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
	
	/** Used to create representation of the cache for debugging. Creates a left justified string of a given length */
	private final String fwid(final String in, final int len)
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

	/** Used to create string representation of the cache for debugging */	
	private final String bytesToString(final byte[] bytes, final int offset, final int length)
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
	//                TABLE LOCK WRAPPER
	//*******************************************************************
	//*******************************************************************
	//*******************************************************************

	/** Queries the number of holds on the table lock by the current thread. */
	public final int getHoldCount() {return tableLock.getHoldCount();}
	
	/** Returns the thread that currently owns the table lock, or null if not owned. */
//	public final Thread getOwner() {return tableLock.getOwner();}
	
	/** Returns a collection containing threads that may be waiting to acquire the table lock. */
//	public final Collection<Thread> getQueuedThreads(){return tableLock.getQueuedThreads();} 

	/** Returns an estimate of the number of threads waiting to acquire the table lock. */
	public final int getQueueLength(){return tableLock.getQueueLength();}

	/** Returns a collection containing those threads that may be waiting on the given condition associated with the table lock. */ 	
//	public final Collection<Thread> getWaitingThreads(Condition condition){return tableLock.getWaitingThreads(condition);} 

	/** Returns an estimate of the number of threads waiting on the given condition associated with the table lock. */ 
	public final int getWaitQueueLength(final Condition condition){ return tableLock.getWaitQueueLength(condition);}

	/** Queries whether the given thread is waiting to acquire the database lock. */
	public final boolean hasQueuedThread(final Thread thread){ return tableLock.hasQueuedThread(thread);}

	/** Queries whether any threads are waiting to acquire the table lock. */
	public final boolean hasQueuedThreads(){ return tableLock.hasQueuedThreads();} 

	/** Queries whether any threads are waiting on the given condition associated with the table lock. */ 
	public final boolean hasWaiters(final Condition condition){ return tableLock.hasWaiters(condition);} 

	/** Returns true because the table lock has fairness set true. */
	public final boolean isFair(){return true;} 

	/** Queries if the table lock is held by the current thread. */
	public final boolean isHeldByCurrentThread(){ return tableLock.isHeldByCurrentThread();}

	/** Queries if the table lock is held by any thread. */
	public final boolean isLocked(){ return tableLock.isLocked();}
	
	/** Acquires the table lock, blocking until it is obtained */
	@Override
	public final void lock() {tableLock.lock();} 
	
	/** Acquires the table lock unless the current thread is interrupted. */
	@Override
	public final void lockInterruptibly() throws InterruptedException
	{tableLock.lockInterruptibly();}
	
	/** Returns a Condition instance for use with the table lock. */
	@Override
	public final Condition newCondition(){return tableLock.newCondition();} 
	
	/** Acquires the table lock only if it is not held by another thread at the time of invocation. */
	@Override
	public final boolean tryLock(){return tableLock.tryLock();}

	/** Acquires the table lock if it is not held by another thread within the given waiting time and the current thread has not been interrupted. */
	@Override
	public final boolean tryLock(final long timeout, final TimeUnit unit){ return tryLock(timeout,unit);} 
	
	/** Attempts to release the table lock. */
	@Override
	public final void unlock(){tableLock.unlock();} 

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

	final void setRemoveOccupancyRatio(final double removeOccupancyRatio) {
		if(operational) return;
		this.removeOccupancyRatio = removeOccupancyRatio;
	}

	final double getCombineOccupancyRatio() {
		return combineOccupancyRatio;
	}

	final void setCombineOccupancyRatio(final double combineOccupancyRatio) {
		if(operational) return;
		this.combineOccupancyRatio = combineOccupancyRatio;
	}

	final int getTableWidth() {
		return tableWidth;
	}

	final RowAccessTypeFactory getRowAccessTypeFactory() {
		return rowAccessTypeFactory;
	}

	final void setRowAccessTypeFactory(final RowAccessTypeFactory rowAccessTypeFactory) {
		this.rowAccessTypeFactory = rowAccessTypeFactory;
		this.rowAccessTypeFactorySet = true;
	}

	public final boolean isBackupNeedsLock() {
		return backupNeedsLock;
	}

	public final void setBackupNeedsLock(final boolean backupNeedsLock) {
		this.backupNeedsLock = backupNeedsLock;
	}

	public final boolean isShutdownNeedsLock() {
		return shutdownNeedsLock;
	}

	public final void setShutdownNeedsLock(final boolean shutdownNeedsLock) {
		this.shutdownNeedsLock = shutdownNeedsLock;
	}

	// Note package scope
	long getTableNumber() {
		return tableNumber;
	}	
}
