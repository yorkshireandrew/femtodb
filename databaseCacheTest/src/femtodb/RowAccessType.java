package femtodb;

public class RowAccessType {
	/** The RowAccessTypeFactory that created this object so it can be reclaimed */
	RowAccessTypeFactory source;
	long primaryKey;
	short flags;
	Table table;
	byte[] byteArray;
	
	RowAccessType(long primaryKey, short flags, Table table, byte[] byteArray, RowAccessTypeFactory source)
	{
		this.primaryKey = primaryKey;
		this.flags = flags;
		this.table = table;
		this.byteArray = byteArray;
		this.source = source;
	}
	
	void close(){
		source.reclaim(this);
	}
}
