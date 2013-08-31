package femtodb;

public class FileMetadata {
	
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
	boolean cached;
	/** The page index if the file is currently loaded in the cache */
	int 	cacheIndex;
	/** The number of rows held within the file */
	int 	rows;
}
