package femtodb;

import java.io.File;

import femtodbexceptions.FemtoDBIOException;
import femtodbexceptions.FemtoDBInvalidValueException;

public class TableDebug1 {

	public static void main(String[] args) throws FemtoDBIOException {
		TableDebug1 test = new TableDebug1();
		test.execute2();
	}
	
	void execute() throws FemtoDBIOException
	{
		FemtoDB fdb = new FemtoDB("debug1");
		fdb.setPath("debug1");
		
		// make a fresh directory
		File f = new File("debug1");
		if(f.exists())f.delete();
		f.mkdir();
		
		// create the table
		Table tut = new Table(fdb, "debugtable1", 0, "pk");
		tut.setRowsPerFile(5);
		tut.setRemoveOccupancyRatio(0.4);
		tut.setCombineOccupancyRatio(0.8);
		tut.addIntegerColumn("payload");
		tut.setCacheSize(140);
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
			tut.insertOrIgnoreByteArrayByPrimaryKey(1L, toInsert);
			System.out.println("Finished First insert");

			// display table
			System.out.println(tut);
			System.out.println(tut.cacheToString());
			
			// insert a load of stuff
			for(int x = 2; x < 15; x++)
			{
				BuffWrite.writeLong(toInsert, 0, (long)x);
				BuffWrite.writeShort(toInsert, 8, (10 * x));
				BuffWrite.writeInt(toInsert, 10, (11 * x));	
				tut.insertOrIgnoreByteArrayByPrimaryKey((long)x, toInsert);
			}
			
			// display table
			System.out.println(tut);
			System.out.println(tut.cacheToString());

			System.out.println("Delete 8");
			tut.deleteByPrimaryKey(8L);
			
			// display table
			System.out.println(tut);
			System.out.println(tut.cacheToString());	
			
			byte[] res = tut.seekByteArray(13L);
			int readValue = BuffRead.readInt(res, 10);
			System.out.println("read = " + readValue);
			
			
			
		} catch (FemtoDBInvalidValueException e) {
			System.out.println(e.toString());
			e.printStackTrace();
		} catch (FemtoDBIOException e) {
			System.out.println(e.toString());
			e.printStackTrace();
		}
	}
	
	
	void execute2() throws FemtoDBIOException
	{
		FemtoDB fdb = new FemtoDB("debug1");
		fdb.setPath("debug1");
		
		// make a fresh directory
		File f = new File("debug1");
		if(f.exists())f.delete();
		f.mkdir();
		
		// create the table
		Table tut = new Table(fdb, "debugtable1", 0, "pk");
		tut.setRowsPerFile(5);
		tut.setRemoveOccupancyRatio(0.4);
		tut.setCombineOccupancyRatio(0.8);
		tut.addIntegerColumn("payload");
		tut.setCacheSize(140);
		try {
			tut.makeOperational();
			
			// first insert
			byte[] toInsert = new byte[8+2+4];
			
			// insert a load of stuff
			for(int x = 1; x < 15; x++)
			{
				BuffWrite.writeLong(toInsert, 0, (long)x);
				BuffWrite.writeShort(toInsert, 8, (10 * x));
				BuffWrite.writeInt(toInsert, 10, (2 * x));	
				tut.insertOrIgnoreByteArrayByPrimaryKey((long)x, toInsert);
			}
			
			// display table
			System.out.println(tut);
			System.out.println(tut.cacheToString());	
			
			System.out.println("========= reading back ===========");	
			// read it back
			for(int x = 1; x < 15; x++)
			{
				toInsert = tut.seekByteArray((long)x);
				if(toInsert == null)System.out.println("" + x + " was null!");
				System.out.println("read =" + BuffRead.readInt(toInsert, 10));
			}	
			
			// delete it all
			for(int x = 1; x < 15; x++)
			{
				tut.deleteByPrimaryKey(x);
			}	
			
			System.out.println("finished delete");
			
			// display table
			System.out.println(tut);
			System.out.println(tut.cacheToString());
			
			// insert a load of stuff
			for(int x = 1; x < 15; x++)
			{
				BuffWrite.writeLong(toInsert, 0, (long)x);
				BuffWrite.writeShort(toInsert, 8, (10 * x));
				BuffWrite.writeInt(toInsert, 10, (3 * x));	
				tut.insertOrIgnoreByteArrayByPrimaryKey((long)x, toInsert);
			}
			
			// read it back
			for(int x = 1; x < 15; x++)
			{
				toInsert = tut.seekByteArray(x);
				System.out.println("read2 =" + BuffRead.readInt(toInsert, 10));
			}				
			
			// delete it all backwards
			for(int x = 14; x >= 1; x--)
			{
				tut.deleteByPrimaryKey(x);
			}			
			
			// display table
			System.out.println(tut);
			System.out.println(tut.cacheToString());
			
		} catch (FemtoDBInvalidValueException e) {
			System.out.println(e.toString());
			e.printStackTrace();
		} catch (FemtoDBIOException e) {
			System.out.println(e.toString());
			e.printStackTrace();
		}
	}

}
