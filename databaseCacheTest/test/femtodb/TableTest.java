package femtodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import femtodbexceptions.InvalidValueException;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TableTest {

	@Before
	public void setUp() throws Exception {

	}
	
	
	@After
	public void tearDown() throws Exception {	
	}
	
	
	//********************** testWriteReadDelete ***********************
	
	@Test
	public void testWriteReadDelete()
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
		tut.setRemoveOccupancyRatio(0.4);
		tut.setCombineOccupancyRatio(0.8);
		tut.addIntegerColumn("payload");
		tut.setCacheSize(140);
		try {
			tut.makeOperational();
			
			// first insert
			byte[] toInsert = new byte[8+2+4];
			BuffWrite.writeLong(toInsert, 0, 1L);
			BuffWrite.writeShort(toInsert, 8, 10);
			BuffWrite.writeInt(toInsert, 10, 11);			
			tut.insertByPrimaryKey(1L, toInsert, 2L);
			
			// check it reads back
			byte[] recalled = tut.seekByPrimaryKey(1L, 3L);
			assertEquals(1L, BuffRead.readLong(recalled, 0));
			assertEquals((short)10, BuffRead.readShort(recalled, 8));
			assertEquals((int)11, BuffRead.readInt(recalled, 10));
			
			// check delete works
			tut.deleteByPrimaryKey(1L, 4L);
			byte[] delTest = tut.seekByPrimaryKey(1L, 5L);
			if(delTest != null){fail();}

		} catch (InvalidValueException e) {
			fail();
		} catch (IOException e) {
			fail();
		}
	}
	
	//********************** testSequentialWriteReadDelete ***********************
	
	@Test
	public void testSequentialWriteReadDelete()
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
		tut.setRemoveOccupancyRatio(0.4);
		tut.setCombineOccupancyRatio(0.8);
		tut.addIntegerColumn("payload");
		tut.setCacheSize(140);
		try {
			tut.makeOperational();
			
			// first insert
			byte[] toInsert = new byte[8+2+4];
			byte[] readBack;
			
			// insert a load of stuff
			for(int x = 1; x < 15; x++)
			{
				BuffWrite.writeLong(toInsert, 0, (long)x);
				BuffWrite.writeShort(toInsert, 8, (10 * x));
				BuffWrite.writeInt(toInsert, 10, (2 * x));	
				tut.insertByPrimaryKey((long)x, toInsert, (long)(x+1));
			}
				
			// read it back
			for(int x = 1; x < 15; x++)
			{
				readBack = tut.seekByPrimaryKey((long)x, 1000 + x);
				int readInt = BuffRead.readInt(readBack, 10);
				assertEquals((2*x), readInt);
			}	
			
			// delete it all
			for(int x = 1; x < 15; x++)
			{
				tut.deleteByPrimaryKey(x, 2000+x);
			}
			
			// check it does not read back
			for(int x = 1; x < 15; x++)
			{
				readBack = tut.seekByPrimaryKey((long)x, 2000 + x);
				if(readBack != null){fail();}
			}
			
			// insert a load of different stuff
			for(int x = 1; x < 15; x++)
			{
				BuffWrite.writeLong(toInsert, 0, (long)x);
				BuffWrite.writeShort(toInsert, 8, (10 * x));
				BuffWrite.writeInt(toInsert, 10, (3 * x));	
				tut.insertByPrimaryKey((long)x, toInsert, (long)(3000 + x));
			}
			
			// read it back
			for(int x = 1; x < 15; x++)
			{
				readBack = tut.seekByPrimaryKey((long)x, 4000 + x);
				int readInt = BuffRead.readInt(readBack, 10);
				assertEquals((3*x), readInt);
			}				
			
			// delete it all backwards
			for(int x = 14; x >= 1; x--)
			{
				tut.deleteByPrimaryKey(x, 5000+x);
			}	
			
			// check it does not read back
			for(int x = 1; x < 15; x++)
			{
				readBack = tut.seekByPrimaryKey((long)x, 2000 + x);
				if(readBack != null){fail();}
			}
			
			
		} catch (InvalidValueException e) {
			fail();
		} catch (IOException e) {
			fail();
		}
	}
	
	
	//********************** RAT based testSequentialWriteReadDelete ***********************
	
	@Test
	public void testSequentialWriteReadDeleteByRAT()
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
		tut.setRemoveOccupancyRatio(0.4);
		tut.setCombineOccupancyRatio(0.8);
		tut.addIntegerColumn("payload");
		tut.setCacheSize(140);
		try {
			tut.makeOperational();
			
			// first insert
			byte[] toInsert = new byte[8+2+4];
			byte[] readBack;
			
			// insert a load of stuff
			for(int x = 1; x < 15; x++)
			{
				BuffWrite.writeLong(toInsert, 0, (long)x);
				BuffWrite.writeShort(toInsert, 8, (10 * x));
				BuffWrite.writeInt(toInsert, 10, (2 * x));
				
				RowAccessType rat = tut.getRowAccessTypeFactory().createRowAccessType(x, tut);
				System.arraycopy(toInsert, 0, rat.byteArray, 0, tut.getTableWidth());		
				tut.insertRATByPrimaryKey((long)x, rat, (long)(x+1));
			}
				
			// read it back
			for(int x = 1; x < 15; x++)
			{
				RowAccessType ratBack = tut.seekRATByPrimaryKey((long)x, 1000 + x);
				readBack = ratBack.byteArray;
				int readInt = BuffRead.readInt(readBack, 10);
				assertEquals((2*x), readInt);
			}	
			
			// delete it all
			for(int x = 1; x < 15; x++)
			{
				tut.deleteByPrimaryKey(x, 2000+x);
			}
			
			// check it does not read back
			for(int x = 1; x < 15; x++)
			{
				readBack = tut.seekByPrimaryKey((long)x, 2000 + x);
				if(readBack != null){fail();}
			}
			
			// insert a load of different stuff
			for(int x = 1; x < 15; x++)
			{
				BuffWrite.writeLong(toInsert, 0, (long)x);
				BuffWrite.writeShort(toInsert, 8, (10 * x));
				BuffWrite.writeInt(toInsert, 10, (3 * x));	
				tut.insertByPrimaryKey((long)x, toInsert, (long)(3000 + x));
			}
			
			// read it back
			for(int x = 1; x < 15; x++)
			{
				readBack = tut.seekByPrimaryKey((long)x, 4000 + x);
				int readInt = BuffRead.readInt(readBack, 10);
				assertEquals((3*x), readInt);
			}				
			
			// delete it all backwards
			for(int x = 14; x >= 1; x--)
			{
				tut.deleteByPrimaryKey(x, 5000+x);
			}	
			
			// check it does not read back
			for(int x = 1; x < 15; x++)
			{
				readBack = tut.seekByPrimaryKey((long)x, 2000 + x);
				if(readBack != null){fail();}
			}
			
			
		} catch (InvalidValueException e) {
			fail();
		} catch (IOException e) {
			fail();
		}
	}
	
	@Test
	public void fastIteratorTest()
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
		tut.setRemoveOccupancyRatio(0.4);
		tut.setCombineOccupancyRatio(0.8);
		tut.addIntegerColumn("payload");
		tut.setCacheSize(140);
		try {
			tut.makeOperational();
			
			// first insert
			byte[] toInsert = new byte[8+2+4];
			RowAccessType rat;
			byte[] readBack;
			
			// insert a load of stuff
			for(int x = 1; x < 15; x++)
			{
				BuffWrite.writeLong(toInsert, 0, (long)(x + 7));
				BuffWrite.writeShort(toInsert, 8, (10 * x));
				BuffWrite.writeInt(toInsert, 10, (2 * x));	
				tut.insertByPrimaryKey((long)(x + 7), toInsert, (long)(x+1));
			}	
			
			FemtoDBIterator fastIterator = tut.fastIterator();
			
			// check fastIterator reads back the table correctly
			for(int x = 1; x < 15; x++)
			{
				assertTrue(fastIterator.hasNext());
				rat = fastIterator.next();
				assertEquals(tut, rat.table);
				assertEquals((long)(x + 7),rat.primaryKey);
				readBack = rat.byteArray;
				assertNotNull(readBack);
				assertEquals((2*x),BuffRead.readInt(readBack, 10));		
			}
			
			// check fastIterator reports the end correctly
			assertFalse(fastIterator.hasNext());
			
			// check fastIterator resets correctly 
			fastIterator.reset();
			
			// check fastIterator reads back the table correctly again
			for(int x = 1; x < 15; x++)
			{
				assertTrue(fastIterator.hasNext());
				rat = fastIterator.next();
				assertEquals(tut, rat.table);
				assertEquals((long)(x + 7),rat.primaryKey);
				readBack = rat.byteArray;
				assertNotNull(readBack);
				assertEquals((2*x),BuffRead.readInt(readBack, 10));		
			}
			
			// check fastIterator reports the end correctly again
			assertFalse(fastIterator.hasNext());
			
			
			
			
		
		} catch (InvalidValueException e) {
			fail();
		} catch (IOException e) {
			fail();
		}
		
	}
	
}
