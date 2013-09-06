package femtodb;

public class RowAccessType {
	long primaryKey;
	Table table;
	byte[] byteArray;
	
	RowAccessType(long primaryKey, Table table, byte[] byteArray)
	{
		this.primaryKey = primaryKey;
		this.table = table;
		this.byteArray = byteArray;
	}
}
