package femtodb;

/** Class that represents a row taken from, or to be inserted into a database tableCore */
public class RowAccessType {
	/** The RowAccessTypeFactory that created this object so it can be reclaimed */
	RowAccessTypeFactory source;
	long primaryKey;
	short flags;
	TableCore tableCore;
	byte[] byteArray;
	
	RowAccessType(long primaryKey, short flags, TableCore tableCore, byte[] byteArray, RowAccessTypeFactory source)
	{
		this.primaryKey = primaryKey;
		this.flags = flags;
		this.tableCore = tableCore;
		this.byteArray = byteArray;
		this.source = source;
	}
	
	void close(){
		source.reclaim(this);
	}
}
