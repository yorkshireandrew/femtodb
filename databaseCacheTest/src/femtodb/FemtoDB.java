package femtodb;

import java.util.ArrayList;
import java.util.List;

public class FemtoDB {

	private String path;
	private long nextUnusedTableNumber;
	List<Table> tables;
	
	FemtoDB()
	{
		tables = new ArrayList<Table>();
		nextUnusedTableNumber 			= 0L;
	}
	
	public Table createTable(String name, String primaryKeyName)
	{
		long tableNumber 	= nextUnusedTableNumber++;
		Table newTable 		= new Table(this, name, tableNumber, primaryKeyName);
		tables.add(newTable);
		return newTable;
	}

	// ****************** Getters and Setters **********************
	
	public final String getPath() {
		return path;
	}

	public final void setPath(String path) {
		this.path = path;
	}
	
	
}
