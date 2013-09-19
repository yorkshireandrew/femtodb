package femtodb;

/** Interface implemented by  factories  that are used by tableCore to provide an unused RowAccessType, as well as recycling ones when it is known that they will no longer be used. */
public interface RowAccessTypeFactory {
	RowAccessType createRowAccessType(long primaryKey, short flags, TableCore tableCore);
	void reclaim(RowAccessType rat);
}
