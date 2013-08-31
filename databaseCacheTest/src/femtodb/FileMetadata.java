package femtodb;

import java.io.File;

public class FileMetadata {
	/** Table */
	Table	owner;
	/** Filename. This gets constructed from file number and owner */
	String	filename;
	/** Value used to identify the file on the file system */
	long 	filenumber;
	/** Lower bound, indicating the smallest primary key that can be put in this file */
	long 	lowerBound;
	/** Upper bound, primary keys must be less than this to be put in this file */
	long 	upperBound;
	/** The smallest primary key value held in the file */
	long 	smallestPK;
	/** The largest primary key value held in the file */
	long 	largestPK;
	/** Flag indicating the file is currently loaded in the cache */
	transient boolean cached;
	/** The page index if the file is currently loaded in the cache */
	transient int 	cacheIndex;
	/** The number of rows held within the file */
	int 	rows;
	/** The ID of the last update to this file. Used for improving the efficiency of backups */
	long 	modificationServiceNumber;
	
	FileMetadata(
			Table owner,
			long filenumber, 
			long lowerBound, 
			long upperBound,
			long smallestPK,
			long largestPK,
			boolean cached,
			int cacheIndex, 
			int rows,
			long modificationServiceNumber)
	{
		this.filenumber = filenumber;
		this.lowerBound = lowerBound;
		this.upperBound = upperBound;
		this.smallestPK = smallestPK;
		this.largestPK = largestPK;
		this.cached = cached;
		this.rows = rows;
		this.modificationServiceNumber = modificationServiceNumber;
		
		filename = owner.database.path + 
				File.pathSeparator + 
				Integer.toString(owner.tableNumber) +
				File.pathSeparator + 
				Long.toString(filenumber);	
	}
}
