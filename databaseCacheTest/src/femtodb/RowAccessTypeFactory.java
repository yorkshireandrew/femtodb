package femtodb;

public interface RowAccessTypeFactory {
	RowAccessType createRowAccessType(long primaryKey, short flags, Table table);
	void reclaim(RowAccessType rat);
}
