package femtodb;

public class CacheMetadata {
	boolean modified;
	long 	lastUsedServiceNumber;
	int		fileMetadataIndex;
	
	CacheMetadata(boolean modified, long lastUsedServiceNumber, int fileMetadataIndex)
	{
		this.modified 				= modified;
		this.lastUsedServiceNumber 	= lastUsedServiceNumber;
		this.fileMetadataIndex 		= fileMetadataIndex;
	}
}

