package femtodb;

public class CacheMetadata {
	boolean modified;
	long 	lastUsed;
	int		fileMetadataIndex;
	
	CacheMetadata(boolean modified, long lastUsed, int fileMetadataIndex)
	{
		this.modified 			= modified;
		this.lastUsed 			= lastUsed;
		this.fileMetadataIndex 	= fileMetadataIndex;
	}
}

