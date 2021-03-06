package femtodb;

import java.io.File;
import java.io.Serializable;

/** Used by the TableCore class, this class holds the meta data information (such as its size and the range of contained primary keys) about one of the data files that are used to store and persist a number of rows of a database tableCore. */
public class FileMetadata implements Serializable{
	private static final long serialVersionUID = 1L;
	
	/** TableCore */
	transient TableCore		owner;
	/** Filename. This gets constructed from file number and owner */
	transient String	filename;
	/** Value used to identify the file on the file system */
	long 				filenumber;
	/** Lower bound, indicating the smallest primary key that can be put in this file */
	long 				lowerBound;
	/** Upper bound, primary keys must be less than this to be put in this file */
	long 				upperBound;
	/** The smallest primary key value held in the file */
	long 				smallestPK;
	/** The largest primary key value held in the file */
	long 				largestPK;
	/** Flag indicating the file is currently loaded in the cache */
	transient boolean 	cached;
	/** The page index if the file is currently loaded in the cache */
	transient int 		cacheIndex;
	/** The number of rows held within the file */
	int 				rows;
	/** The ServiceNumber of the last update to this file. Used for improving the efficiency of backups */
	long 				modificationServiceNumber;
	
	// Fields used for cache management 
	/** the ServiceNumber of the last use of the cache page */
	long 				lastUsedServiceNumber;
	
	/** Set true if the cache page has been modified since it was loaded from disk */
	boolean				modified;
	
	FileMetadata(
			final TableCore owner,
			final long filenumber, 
			final long lowerBound, 
			final long upperBound,
			final long smallestPK,
			final long largestPK,
			final boolean cached,
			final int cacheIndex, 
			final int rows,
			final long modificationServiceNumber)
	{
		this.owner = owner;
		this.filenumber = filenumber;
		this.lowerBound = lowerBound;
		this.upperBound = upperBound;
		this.smallestPK = smallestPK;
		this.largestPK = largestPK;
		this.cached = cached;
		this.rows = rows;
		this.modificationServiceNumber = modificationServiceNumber;
		
		filename = owner.getTableDirectory() + File.separator +
				Long.toString(filenumber);	
	}
	
	@Override
	public String toString()
	{
		String retval = "";
		retval = retval + "filename: " + filename + "\n";
		retval = retval + "lower bound: " + lowerBound + "\n";
		retval = retval + "upper bound: " + upperBound + "\n";
		retval = retval + "smallest PK: " + smallestPK + "\n";
		retval = retval + "largest PK: " + largestPK + "\n";
		retval = retval + "cached: " + cached + "\n";
		retval = retval + "cache Index: " + cacheIndex + "\n";
		retval = retval + "rows: " + rows + "\n";
		retval = retval + "modificationServiceNumber: " + modificationServiceNumber + "\n";
		retval = retval + "lastUsedServiceNumber: " + lastUsedServiceNumber;
		return retval;	
	}
	
    final void finishLoading(TableCore newOwner)
    {
		owner 		= newOwner;
    	filename 	= owner.getTableDirectory() + File.separator +
						Long.toString(filenumber);
 //   	System.out.println("Filenumber " + filenumber + " setting filename to " + filename);
 //   	System.out.println(this.hashCode());
    	cached 		= false;
		cacheIndex 	= -1;
    }
}
