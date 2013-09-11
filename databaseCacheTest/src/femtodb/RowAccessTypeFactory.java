package femtodb;

public interface RowAccessTypeFactory {
	RowAccessType createRowAccessType(long primaryKey, Table table);
	void reclaim(RowAccessType rat);
}
