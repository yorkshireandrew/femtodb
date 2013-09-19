package femtodb;

import java.io.Serializable;

/** Default implementation of RowAccessTypeFactory creating RowAccessTypes from the heap and normal garbage collection for reclamation */
public class DefaultRowAccessTypeFactory implements RowAccessTypeFactory, Serializable {
	private static final long serialVersionUID = 1L;

	DefaultRowAccessTypeFactory(int byteArrayLength)
	{}
	
	@Override
	public RowAccessType createRowAccessType(long primaryKey, short flags, TableCore tableCore) {
		byte[] byteArray = new byte[tableCore.getTableWidth()];
		return new RowAccessType(primaryKey, flags, tableCore,
			byteArray, this);
	}

	@Override
	public void reclaim(RowAccessType rat) {
		// Do nothing - Leave to garbage collector to reclaim

	}

}
