package femtodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import femtodbexceptions.FemtoDBConcurrentModificationException;
import femtodbexceptions.FemtoDBIOException;
import femtodbexceptions.FemtoDBInvalidValueException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class TableTest {

	@Before
	public void setUp() throws Exception {

	}
	
	@After
	public void tearDown() throws Exception {	
	}
	
	
	//********************** testWriteReadDelete ***********************

	@Test
	public void testWriteReadDelete() throws FemtoDBIOException
	{
		FemtoDB fdb = new FemtoDB("debug1");
		fdb.setPath("debug1");
		
		// make a fresh directory
		File f = new File("debug1");
		if(f.exists())f.delete();
		f.mkdir();
		
		// create the tableCore
		TableCore tut = new TableCore(fdb, "debugtable1", 0, "pk");
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
			tut.insertOrIgnoreByteArrayByPrimaryKey(1L, toInsert);
			
			// check it reads back
			byte[] recalled = tut.seekByteArray(1L);
			assertEquals(1L, BuffRead.readLong(recalled, 0));
			assertEquals((short)10, BuffRead.readShort(recalled, 8));
			assertEquals((int)11, BuffRead.readInt(recalled, 10));
			
			// check delete works
			tut.deleteByPrimaryKey(1L);
			byte[] delTest = tut.seekByteArray(1L);
			if(delTest != null){fail();}

		} catch (FemtoDBInvalidValueException e) {
			fail();
		} catch (FemtoDBIOException e) {
			fail();
		}
	}
	
	//********************** testSequentialWriteReadDelete ***********************
	
	@Test
	public void testSequentialWriteReadDelete()
	{	
		try{
		FemtoDB fdb = new FemtoDB("debug1");
		fdb.setPath("debug1");
		
		// make a fresh directory
		File f = new File("debug1");
		if(f.exists())f.delete();
		f.mkdir();
		
		// create the tableCore
		TableCore tut = new TableCore(fdb, "debugtable1", 0, "pk");
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
				tut.insertOrIgnoreByteArrayByPrimaryKey((long)x, toInsert);
			}
				
			// read it back
			for(int x = 1; x < 15; x++)
			{
				readBack = tut.seekByteArray((long)x);
				int readInt = BuffRead.readInt(readBack, 10);
				assertEquals((2*x), readInt);
			}	
			
			// delete it all
			for(int x = 1; x < 15; x++)
			{
				tut.deleteByPrimaryKey(x);
			}
			
			// check it does not read back
			for(int x = 1; x < 15; x++)
			{
				readBack = tut.seekByteArray((long)x);
				if(readBack != null){fail();}
			}
			
			// insert a load of different stuff
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
				readBack = tut.seekByteArray((long)x);
				int readInt = BuffRead.readInt(readBack, 10);
				assertEquals((3*x), readInt);
			}				
			
			// delete it all backwards
			for(int x = 14; x >= 1; x--)
			{
				tut.deleteByPrimaryKey(x);
			}	
			
			// check it does not read back
			for(int x = 1; x < 15; x++)
			{
				readBack = tut.seekByteArray((long)x);
				if(readBack != null){fail();}
			}
			
			
		} catch (FemtoDBInvalidValueException e) {
			fail();
		} catch (FemtoDBIOException e) {
			fail();
		}
		}catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
		}
	}
	
	
	//********************** RAT based testSequentialWriteReadDelete ***********************
	
	@Test
	public void testSequentialWriteReadDeleteByRAT() throws FemtoDBIOException
	{	
		FemtoDB fdb = new FemtoDB("debug1");
		fdb.setPath("debug1");
		
		// make a fresh directory
		File f = new File("debug1");
		if(f.exists())f.delete();
		f.mkdir();
		
		// create the tableCore
		TableCore tut = new TableCore(fdb, "debugtable1", 0, "pk");
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
				
				RowAccessType rat = tut.getRowAccessTypeFactory().createRowAccessType(x, TableCore.FLAG_CACHE_NOT_SET, tut);
				System.arraycopy(toInsert, 0, rat.byteArray, 0, tut.getTableWidth());		
				tut.insertOrIgnore((long)x, rat);
			}
				
			// read it back
			for(int x = 1; x < 15; x++)
			{
				RowAccessType ratBack = tut.seek((long)x);
				readBack = ratBack.byteArray;
				int readInt = BuffRead.readInt(readBack, 10);
				assertEquals((2*x), readInt);
			}	
			
			// delete it all
			for(int x = 1; x < 15; x++)
			{
				tut.deleteByPrimaryKey(x);
			}
			
			// check it does not read back
			for(int x = 1; x < 15; x++)
			{
				readBack = tut.seekByteArray((long)x);
				if(readBack != null){fail();}
			}
			
			// insert a load of different stuff
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
				readBack = tut.seekByteArray((long)x);
				int readInt = BuffRead.readInt(readBack, 10);
				assertEquals((3*x), readInt);
			}				
			
			// delete it all backwards
			for(int x = 14; x >= 1; x--)
			{
				tut.deleteByPrimaryKey(x);
			}	
			
			// check it does not read back
			for(int x = 1; x < 15; x++)
			{
				readBack = tut.seekByteArray((long)x);
				if(readBack != null){fail();}
			}
			
			
		} catch (FemtoDBInvalidValueException e) {
			fail();
		} catch (FemtoDBIOException e) {
			fail();
		}
	}
	
	@Test
	public void fastIteratorTest() throws FemtoDBIOException
	{
		FemtoDB fdb = new FemtoDB("debug1");
		fdb.setPath("debug1");
		
		// make a fresh directory
		File f = new File("debug1");
		if(f.exists())f.delete();
		f.mkdir();
		
		// create the tableCore
		TableCore tut = new TableCore(fdb, "debugtable1", 0, "pk");
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
				tut.insertOrIgnoreByteArrayByPrimaryKey((long)(x + 7), toInsert);
			}	
			
			FemtoDBIterator fastIterator = tut.fastIterator();
			
			// check fastIterator reads back the tableCore correctly
			for(int x = 1; x < 15; x++)
			{
				assertTrue(fastIterator.hasNext());
				rat = fastIterator.next();
				assertEquals(tut, rat.tableCore);
				assertEquals((long)(x + 7),rat.primaryKey);
				readBack = rat.byteArray;
				assertNotNull(readBack);
				assertEquals((2*x),BuffRead.readInt(readBack, 10));		
			}
			
			// check fastIterator reports the end correctly
			assertFalse(fastIterator.hasNext());
			
			// check fastIterator resets correctly 
			fastIterator.reset();
			
			// check fastIterator reads back the tableCore correctly again
			for(int x = 1; x < 15; x++)
			{
				assertTrue(fastIterator.hasNext());
				rat = fastIterator.next();
				assertEquals(tut, rat.tableCore);
				assertEquals((long)(x + 7),rat.primaryKey);
				readBack = rat.byteArray;
				assertNotNull(readBack);
				assertEquals((2*x),BuffRead.readInt(readBack, 10));		
			}
			
			// check fastIterator reports the end correctly again
			assertFalse(fastIterator.hasNext());
			
			
			
			
		
		} catch (FemtoDBInvalidValueException e) {
			fail();
		} catch (FemtoDBIOException e) {
			fail();
		} catch (FemtoDBConcurrentModificationException e) {
			fail();
		}
		
	}
	
	
	@Test
	public void SafeIteratorTest() throws FemtoDBIOException
	{
		FemtoDB fdb = new FemtoDB("debug1");
		fdb.setPath("debug1");
		
		// make a fresh directory
		File f = new File("debug1");
		if(f.exists())f.delete();
		f.mkdir();
		
		// create the tableCore
		TableCore tut = new TableCore(fdb, "debugtable1", 0, "pk");
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
				tut.insertOrIgnoreByteArrayByPrimaryKey((long)(x + 7), toInsert);
			}	
			
			FemtoDBIterator safeIterator = tut.safeIterator();
			
			// check fastIterator reads back the tableCore correctly
			for(int x = 1; x < 15; x++)
			{
				assertTrue(safeIterator.hasNext());
				rat = safeIterator.next();
				assertEquals(tut, rat.tableCore);
				assertEquals((long)(x + 7),rat.primaryKey);
				readBack = rat.byteArray;
				assertNotNull(readBack);
				assertEquals((2*x),BuffRead.readInt(readBack, 10));		
			}
			
			// check fastIterator reports the end correctly
			assertFalse(safeIterator.hasNext());
			
			// check fastIterator resets correctly 
			safeIterator.reset();
			
			// check fastIterator reads back the tableCore correctly again
			for(int x = 1; x < 15; x++)
			{
				assertTrue(safeIterator.hasNext());
				rat = safeIterator.next();
				assertEquals(tut, rat.tableCore);
				assertEquals((long)(x + 7),rat.primaryKey);
				readBack = rat.byteArray;
				assertNotNull(readBack);
				assertEquals((2*x),BuffRead.readInt(readBack, 10));		
			}
			
			// check fastIterator reports the end correctly again
			assertFalse(safeIterator.hasNext());
			
			
			
			
		
		} catch (FemtoDBInvalidValueException e) {
			fail();
		} catch (FemtoDBIOException e) {
			fail();
		} catch (FemtoDBConcurrentModificationException e) {
			fail();
		}
		
	}
	

	
	/** tests deleteing stuff the interator has travelled over 
	 * @throws FemtoDBIOException */

	@Test
	public void SafeIteratorTest2() throws FemtoDBIOException
	{
		FemtoDB fdb = new FemtoDB("debug1");
		fdb.setPath("debug1");
		
		// make a fresh directory
		File f = new File("debug1");
		if(f.exists())f.delete();
		f.mkdir();
		
		// create the tableCore
		TableCore tut = new TableCore(fdb, "debugtable1", 0, "pk");
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
				tut.insertOrIgnoreByteArrayByPrimaryKey((long)(x + 7), toInsert);
			}	
			
			FemtoDBIterator safeIterator = tut.safeIterator();
			
			// check safeIterator reads back the tableCore correctly
			for(int x = 1; x < 10; x++)
			{
				assertTrue(safeIterator.hasNext());
				rat = safeIterator.next();
				assertEquals(tut, rat.tableCore);
				assertEquals((long)(x + 7),rat.primaryKey);
				readBack = rat.byteArray;
				assertNotNull(readBack);
				assertEquals((2*x),BuffRead.readInt(readBack, 10));		
			}
			
			System.out.println("****** prior to delete *******");
			System.out.println(tut);
			System.out.println(tut.cacheToString());
			System.out.println("****** *************** *******");
			
			// delete a load of stuff before iterator
			for(int x = 1; x < 8; x++)
			{
				tut.deleteByPrimaryKey((long)(x+7) );
			}
			
			System.out.println("****** after delete *******");
			System.out.println(tut);
			System.out.println(tut.cacheToString());
			System.out.println("****** *************** *******");
			
			// carry on iterating
			for(int x = 10; x < 15; x++)
			{
				System.out.println("x = " + x);
				assertTrue(safeIterator.hasNext());
				rat = safeIterator.next();
				assertEquals(tut, rat.tableCore);
				assertEquals((long)(x + 7),rat.primaryKey);
				readBack = rat.byteArray;
				assertNotNull(readBack);
				assertEquals((2*x),BuffRead.readInt(readBack, 10));		
			}
			
			// check safeIterator reports the end correctly
			assertFalse(safeIterator.hasNext());
			
			// check safeIterator resets correctly 
			safeIterator.reset();
			
			// check fastIterator reads back the tableCore correctly again
			for(int x = 8; x < 15; x++)
			{
				assertTrue(safeIterator.hasNext());
				rat = safeIterator.next();
				assertEquals(tut, rat.tableCore);
				assertEquals((long)(x + 7),rat.primaryKey);
				readBack = rat.byteArray;
				assertNotNull(readBack);
				assertEquals((2*x),BuffRead.readInt(readBack, 10));		
			}
			
			// check fastIterator reports the end correctly again
			assertFalse(safeIterator.hasNext());
		} catch (FemtoDBInvalidValueException e) {
			fail();
		} catch (FemtoDBIOException e) {
			System.out.println(e.toString());
			e.printStackTrace();
			fail();
		} catch (FemtoDBConcurrentModificationException e) {
			fail();
		}
	}
	
	@Test
	public void SafeIteratorTest3() throws FemtoDBIOException
	{
		FemtoDB fdb = new FemtoDB("debug1");
		fdb.setPath("debug1");
		
		// make a fresh directory
		File f = new File("debug1");
		if(f.exists())f.delete();
		f.mkdir();
		
		// create the tableCore
		TableCore tut = new TableCore(fdb, "debugtable1", 0, "pk");
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
				tut.insertOrIgnoreByteArrayByPrimaryKey((long)(x + 7), toInsert);
			}	
			
			FemtoDBIterator safeIterator = tut.safeIterator();
			
			// check fastIterator reads back the tableCore correctly
			for(int x = 1; x < 10; x++)
			{
				assertTrue(safeIterator.hasNext());
				rat = safeIterator.next();
				assertEquals(tut, rat.tableCore);
				assertEquals((long)(x + 7),rat.primaryKey);
				readBack = rat.byteArray;
				assertNotNull(readBack);
				assertEquals((2*x),BuffRead.readInt(readBack, 10));		
			}
			
			// delete a load of stuff before iterator
			for(int x = 12; x < 15; x++)
			{
				tut.deleteByPrimaryKey((long)(x+7) );
			}
			
			int x;
			
			x = 10;
				assertTrue(safeIterator.hasNext());
				rat = safeIterator.next();
				assertEquals(tut, rat.tableCore);
				assertEquals((long)(x + 7),rat.primaryKey);
				readBack = rat.byteArray;
				assertNotNull(readBack);
				assertEquals((2*x),BuffRead.readInt(readBack, 10));		

			x = 11;
				assertTrue(safeIterator.hasNext());
				rat = safeIterator.next();
				assertEquals(tut, rat.tableCore);
				assertEquals((long)(x + 7),rat.primaryKey);
				readBack = rat.byteArray;
				assertNotNull(readBack);
				assertEquals((2*x),BuffRead.readInt(readBack, 10));		
			
			// check fastIterator reports the end correctly
			assertFalse(safeIterator.hasNext());
			
			// check fastIterator resets correctly 
			safeIterator.reset();
			
			// check fastIterator reads back the tableCore correctly again
			for(int xx = 1; xx < 12; xx++)
			{
				assertTrue(safeIterator.hasNext());
				rat = safeIterator.next();
				assertEquals(tut, rat.tableCore);
				assertEquals((long)(xx + 7),rat.primaryKey);
				readBack = rat.byteArray;
				assertNotNull(readBack);
				assertEquals((2*xx),BuffRead.readInt(readBack, 10));		
			}
			
			// check fastIterator reports the end correctly again
			assertFalse(safeIterator.hasNext());
		} catch (FemtoDBInvalidValueException e) {
			fail();
		} catch (FemtoDBIOException e) {
			fail();
		} catch (FemtoDBConcurrentModificationException e) {
			fail();
		}
	}

	@Test
	public void SafeIteratorTest4() throws FemtoDBIOException
	{
		FemtoDB fdb = new FemtoDB("debug1");
		fdb.setPath("debug1");
		
		// make a fresh directory
		File f = new File("debug1");
		if(f.exists())f.delete();
		f.mkdir();
		
		// create the tableCore
		TableCore tut = new TableCore(fdb, "debugtable1", 0, "pk");
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
			
				int x = 1;
				BuffWrite.writeLong(toInsert, 0, (long)(x + 7));
				BuffWrite.writeShort(toInsert, 8, (10 * x));
				BuffWrite.writeInt(toInsert, 10, (2 * x));	
				tut.insertOrIgnoreByteArrayByPrimaryKey((long)(x + 7), toInsert);

			
				FemtoDBIterator safeIterator = tut.safeIterator();
			
			// check fastIterator reads back the tableCore correctly

				try {
					assertTrue(safeIterator.hasNext());
					rat = safeIterator.next();
					assertEquals(tut, rat.tableCore);
					assertEquals((long)(x + 7),rat.primaryKey);
					readBack = rat.byteArray;
					assertNotNull(readBack);
					assertEquals((2*x),BuffRead.readInt(readBack, 10));
				} catch (FemtoDBConcurrentModificationException e) {
					fail();
				}
				System.out.println("*******************************************");
				System.out.println("*******************************************");
				System.out.println("*******************************************");
				System.out.println("*******************************************");
				
				System.out.println("SafeIteratorTest4() deleting");
				tut.deleteByPrimaryKey((long)(x+7) );
				System.out.println("SafeIteratorTest4() deleting complete");
				System.out.println(tut);
				System.out.println(tut.cacheToString());
				
				// should throw exception as current row gone
				try {
					assertTrue(safeIterator.hasNext());
					fail();
				} catch (FemtoDBConcurrentModificationException e) {
				}
				
				// should throw exception as current row gone
				try {
					safeIterator.next();
					fail();
				} catch (FemtoDBConcurrentModificationException e) {
				}

		} catch (FemtoDBInvalidValueException e) {
			fail();
		} catch (FemtoDBIOException e) {
			fail();
		}
	}

	@Test
	public void SafeIteratorTest5() throws FemtoDBIOException
	{
		FemtoDB fdb = new FemtoDB("debug1");
		fdb.setPath("debug1");
		
		// make a fresh directory
		File f = new File("debug1");
		if(f.exists())f.delete();
		f.mkdir();
		
		// create the tableCore
		TableCore tut = new TableCore(fdb, "debugtable1", 0, "pk");
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
			
				int x = 1;
				BuffWrite.writeLong(toInsert, 0, (long)(x + 7));
				BuffWrite.writeShort(toInsert, 8, (10 * x));
				BuffWrite.writeInt(toInsert, 10, (2 * x));	
				tut.insertOrIgnoreByteArrayByPrimaryKey((long)(x + 7), toInsert);

				x = 2;
				BuffWrite.writeLong(toInsert, 0, (long)(x + 7));
				BuffWrite.writeShort(toInsert, 8, (10 * x));
				BuffWrite.writeInt(toInsert, 10, (2 * x));	
				tut.insertOrIgnoreByteArrayByPrimaryKey((long)(x + 7), toInsert);

				x = 3;
				BuffWrite.writeLong(toInsert, 0, (long)(x + 7));
				BuffWrite.writeShort(toInsert, 8, (10 * x));
				BuffWrite.writeInt(toInsert, 10, (2 * x));	
				tut.insertOrIgnoreByteArrayByPrimaryKey((long)(x + 7), toInsert);

				FemtoDBIterator safeIterator = tut.safeIterator();
			

				x = 1;
				try {
					assertTrue(safeIterator.hasNext());
					rat = safeIterator.next();
					assertEquals(tut, rat.tableCore);
					assertEquals((long)(x + 7),rat.primaryKey);
					readBack = rat.byteArray;
					assertNotNull(readBack);
					assertEquals((2*x),BuffRead.readInt(readBack, 10));
				} catch (FemtoDBConcurrentModificationException e) {
					fail();
				}
				
				x = 2;
				try {
					assertTrue(safeIterator.hasNext());
					rat = safeIterator.next();
					assertEquals(tut, rat.tableCore);
					assertEquals((long)(x + 7),rat.primaryKey);
					readBack = rat.byteArray;
					assertNotNull(readBack);
					assertEquals((2*x),BuffRead.readInt(readBack, 10));
				} catch (FemtoDBConcurrentModificationException e) {
					fail();
				}
				
				// remove what iterator is currently on - it should go back
				try {
					safeIterator.remove();
				} catch (UnsupportedOperationException e1) {
					fail();
				} catch (FemtoDBConcurrentModificationException e1) {
					fail();
				}
				System.out.println(" *** REMOVE COMPLETED **** ");
			
				x = 3;
				try {
					assertTrue(safeIterator.hasNext());
					rat = safeIterator.next();
					if(rat == null)System.out.println("rat was null!");
					assertEquals(tut, rat.tableCore);
					assertEquals((long)(x + 7),rat.primaryKey);
					readBack = rat.byteArray;
					assertNotNull(readBack);
					assertEquals((2*x),BuffRead.readInt(readBack, 10));
				} catch (FemtoDBConcurrentModificationException e) {
					fail();
				}


		} catch (FemtoDBInvalidValueException e) {
			fail();
		} catch (FemtoDBIOException e) {
			fail();
		}
	}
	
	@Test
	public void testSerialisation() throws FemtoDBIOException
	{
		FemtoDB fdb = new FemtoDB("debug1");
		fdb.setPath("debug1");
		
		// make a fresh directory
		File f = new File("debug1");
		if(f.exists())f.delete();
		f.mkdir();
		
		// create the tableCore
		TableCore tut = new TableCore(fdb, "debugtable1", 0, "pk");
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
		} catch (FemtoDBInvalidValueException e) {
			fail();
		} catch (FemtoDBIOException e) {
			fail();
		}
		
		// save the tableCore
		try {
			tut.flushCache();
			
			File blah = new File("test_table");
			if(blah.exists())blah.delete();
			FileOutputStream os = new FileOutputStream("test_table");
			ObjectOutputStream oos = new ObjectOutputStream(os);
			oos.writeObject(tut);
			oos.close();	
		} catch (FileNotFoundException e) {
			fail();
		} catch (IOException e) {
			System.out.println(e);
			e.printStackTrace();
			fail();
		} catch (FemtoDBIOException e) {
			fail();
		}
		
		// ensure old database reference is gone
		fdb = null;
		
		// move the database data to a new location
		File f2 = new File("debug1");
		File f3 = new File("debug2");
		if(f3.exists())FileUtils.recursiveDelete(f3);
		
		assertTrue(f2.renameTo( new File("debug2")));
		
		// create a new database pointing to the new location
		FemtoDB fdb2 = new FemtoDB("debug1");
		fdb2.setPath("debug2");
		
		
		// create a new tableCore
		TableCore tut2 = null;
		try {
			FileInputStream is = new FileInputStream("test_table");
			ObjectInputStream ois = new ObjectInputStream(is);
			tut2 = (TableCore)ois.readObject();
			ois.close();	
		} catch (FileNotFoundException e) {
			fail();
		} catch (IOException e) {
			fail();
		} catch (ClassNotFoundException e) {
			fail();
		}
		
		tut2.finishLoading(fdb2);
		System.out.println(" reading tablecontents back after save load");
		// read contents back
		try{
			byte[] readBack;
			for(int x = 1; x < 15; x++)
			{
				readBack = tut2.seekByteArray((long)x);
				int readInt = BuffRead.readInt(readBack, 10);
				assertEquals((2*x), readInt);
			}		
		}
		catch(FemtoDBIOException e)
		{
			System.out.println(e);
			e.printStackTrace();
			fail();			
		}
	}
	
}
