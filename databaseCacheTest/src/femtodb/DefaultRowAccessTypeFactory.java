package femtodb;

public class DefaultRowAccessTypeFactory implements RowAccessTypeFactory {

	DefaultRowAccessTypeFactory(int byteArrayLength)
	{}
	
	@Override
	public RowAccessType createRowAccessType(long primaryKey, short flags, Table table) {
		byte[] byteArray = new byte[table.getTableWidth()];
		return new RowAccessType(primaryKey, flags, table,
			byteArray, this);
	}

	@Override
	public void reclaim(RowAccessType rat) {
		// Do nothing - Leave to garbage collector to reclaim

	}

}
