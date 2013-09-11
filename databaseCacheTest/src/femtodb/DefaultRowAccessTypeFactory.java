package femtodb;

public class DefaultRowAccessTypeFactory implements RowAccessTypeFactory {

	DefaultRowAccessTypeFactory(int byteArrayLength)
	{}
	
	@Override
	public RowAccessType createRowAccessType(long primaryKey, Table table) {
		byte[] byteArray = new byte[table.getTableWidth()];
		return new RowAccessType(primaryKey, table,
			byteArray, this);
	}

	@Override
	public void reclaim(RowAccessType rat) {
		// Do nothing - Leave to garbage collector to reclaim

	}

}
