package femtodb;

import java.io.File;
import java.io.IOException;

import femtodbexceptions.InvalidValueException;

public class TableDebug1 {

	public static void main(String[] args) {
		TableDebug1 test = new TableDebug1();
		test.execute();
	}
	
	void execute()
	{
		FemtoDB fdb = new FemtoDB();
		fdb.path = "debug1";
		
		// make a fresh directory
		File f = new File("debug1");
		if(f.exists())f.delete();
		f.mkdir();
		
		// create the table
		Table tut = new Table(fdb, "debugtable1", 0, "pk");
		tut.setRowsPerFile(5);
		tut.setRemoveOccupancyRatio(0.2);
		tut.setCombineOccupancyRatio(0.4);
		tut.addIntegerColumn("payload");
		tut.setCacheSize(70);
		try {
			tut.makeOperational();
			System.out.println(tut);
			System.out.println(tut.cacheToString());
			
			// first insert
			byte[] toInsert = new byte[8+2+4];
			BuffWrite.writeLong(toInsert, 0, 1L);
			BuffWrite.writeShort(toInsert, 8, 10);
			BuffWrite.writeInt(toInsert, 10, 11);
			
			System.out.println("Starting First insert");
			tut.insertByPrimaryKey(1L, toInsert, 2L);
			System.out.println("Finished First insert");

			// display table
			System.out.println(tut);
			System.out.println(tut.cacheToString());
			
			// insert a load of stuff
			for(int x = 2; x < 12; x++)
			{
				BuffWrite.writeLong(toInsert, 0, (long)x);
				BuffWrite.writeShort(toInsert, 8, (10 * x));
				BuffWrite.writeInt(toInsert, 10, (11 * x));	
				tut.insertByPrimaryKey((long)x, toInsert, (long)(x+1));
			}
			
			// display table
			System.out.println(tut);
			System.out.println(tut.cacheToString());

			
			
			
			
			
		} catch (InvalidValueException e) {
			System.out.println(e.toString());
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println(e.toString());
			e.printStackTrace();
		}
		
		
	}

}
