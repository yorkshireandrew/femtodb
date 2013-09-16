package femtodb;

import java.io.Serializable;

public class DefaultRowAccessTypeFactory implements RowAccessTypeFactory, Serializable {
	private static final long serialVersionUID = 1L;

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
