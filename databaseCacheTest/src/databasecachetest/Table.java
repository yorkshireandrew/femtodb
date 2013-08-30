package databasecachetest;

import java.util.Arrays;

import femtodbexceptions.AddedColumnToOperationalTableException;
import femtodbexceptions.FemtoDBException;

public class Table {

	/** Arrays regarding columns */
	String[]	columnNames;
	int[]		columnByteOffset;
	int[]		columnByteWidth;
	int			tableWidth;
	boolean		operationalTable = false;
	
	Table(String name)
	{
		columnNames 		= new String[0];
		columnByteOffset 	= new int[0];
		columnByteWidth		= new int[0];
		tableWidth = 0;
	}
	
	void addByteColumn(String columnName) throws FemtoDBException
	{
		if(operationalTable) throw new AddedColumnToOperationalTableException();
		columnNames = addToArray(columnNames, columnName);
		columnByteOffset = addToArray(columnByteOffset,tableWidth);
		columnByteWidth = addToArray(columnByteWidth,1);
		tableWidth++;
	}
	
	private int[] addToArray(int[] in, int toAdd)
	{
		int len = in.length;
		int[] retval = Arrays.copyOf(in, len+1);
		retval[len] = toAdd;
		return retval;
	}
	
	private String[] addToArray(String[] in, String toAdd)
	{
		int len = in.length;
		String[] retval = Arrays.copyOf(in, len+1);
		retval[len] = toAdd;
		return retval;
	}
}
