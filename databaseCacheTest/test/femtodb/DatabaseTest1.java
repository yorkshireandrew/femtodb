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
import femtodbexceptions.FemtoDBShuttingDownException;
import femtodbexceptions.FemtoDBTableDeletedException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class DatabaseTest1 {

	@Before
	public void setUp() throws Exception {

	}
	
	@After
	public void tearDown() throws Exception {	
	}
	
	
	// ************************************************
	// ************************************************
	// **********     testShutdownOpen()     ********** 
	// ************************************************
	// ************************************************

	@Test
	public void testShutdownOpen()
	{	
		System.out.println("DatabaseTest1 - testShutdownOpen()");
		try{
			// ***************** CREATE DATABASE TO SHUTDOWN ******************
			FemtoDB fdb = new FemtoDB("debug1");
			
			// make a fresh directory
			File f = new File("debug1");
			if(f.exists())FileUtils.recursiveDelete(f);
			f.mkdir();
			
			// this must follow the making of the directory!
			fdb.setPath("debug1");
			
			// ensure backup does not exist
			File fbackup = new File("debug1backup");
			if(fbackup.exists())FileUtils.recursiveDelete(fbackup);
			
			// create a table in the database
			TableCore table1 =  fdb.createTable("table1", "pk");
			
			// make the table operational
			table1.setRowsPerFile(5);
			table1.setRemoveOccupancyRatio(0.4);
			table1.setCombineOccupancyRatio(0.8);
			table1.addIntegerColumn("payload");
			table1.setCacheSize(140);
			table1.makeOperational();
				
			// insert a load of stuff
			byte[] toInsert = new byte[8+2+4];
			for(int x = 1; x < 15; x++)
			{
				BuffWrite.writeLong(toInsert, 0, (long)x);
				BuffWrite.writeShort(toInsert, 8, (10 * x));
				BuffWrite.writeInt(toInsert, 10, (2 * x));	
				table1.insertOrIgnoreByteArrayByPrimaryKey((long)x, toInsert);
			}
			
			// *****************  SHUTDOWN DATABASE ******************
			fdb.shutdown();
			
			// *****************  OPEN THE SHUTDOWN DATABASE *********
			FemtoDB fdb2 = FemtoDB.open("debug1", null);
			TableCore table2 =  fdb2.getTable("table1");
			assertNotNull(table2);
			
			// read back database contents		
			byte[] readBack;
			for(int x = 1; x < 15; x++)
			{
				readBack = table2.seekByteArray((long)x);
				int readInt = BuffRead.readInt(readBack, 10);
				assertEquals((2*x), readInt);
			}			
			
		// catch any stuff going wrong
		}
		catch (FemtoDBInvalidValueException e) {fail();} 
		catch (FemtoDBIOException e) {
			System.err.println(e);
			e.printStackTrace();
			fail();}
		catch (Exception e)
		{
			System.err.println(e);
			e.printStackTrace();
			fail();
		}
	}
	

	
	// ************************************************
	// ************************************************
	// **********     testBackupOpen()       ********** 
	// ************************************************
	// ************************************************	
	@Test
	public void testBackupOpen()
	{	
		System.out.println("DatabaseTest1 - testBackupOpen()");
		try{
			// ***************** CREATE DATABASE TO SHUTDOWN ******************
			FemtoDB fdb = new FemtoDB("debug1");
			fdb.setBackupDirectory("debug1backup");
			
			// make a fresh directory
			File f = new File("debug1");
			if(f.exists())FileUtils.recursiveDelete(f);
			f.mkdir();
			
			// this must follow the making of the directory!
			fdb.setPath("debug1");
			
			// ensure backup does not exist
			File fbackup = new File("debug1backup");
			if(fbackup.exists())FileUtils.recursiveDelete(fbackup);
			
			// create a table in the database
			TableCore table1 =  fdb.createTable("table1", "pk");
			
			// make the table operational
			table1.setRowsPerFile(5);
			table1.setRemoveOccupancyRatio(0.4);
			table1.setCombineOccupancyRatio(0.8);
			table1.addIntegerColumn("payload");
			table1.setCacheSize(140);
			table1.makeOperational();
				
			// insert a load of stuff
			byte[] toInsert = new byte[8+2+4];
			for(int x = 1; x < 15; x++)
			{
				BuffWrite.writeLong(toInsert, 0, (long)x);
				BuffWrite.writeShort(toInsert, 8, (10 * x));
				BuffWrite.writeInt(toInsert, 10, (2 * x));	
				table1.insertOrIgnoreByteArrayByPrimaryKey((long)x, toInsert);
			}
			
			// *****************  SHUTDOWN DATABASE ******************
			fdb.backup();
			fdb.shutdown();
			
			// *****************  OPEN THE SHUTDOWN DATABASE *********
			// make a fresh directory to restore database back
			File f2 = new File("debug1");
			if(f2.exists())FileUtils.recursiveDelete(f2);
			f2.mkdir();
			
			FemtoDB fdb2 = FemtoDB.open("debug1", "debug1backup");
			TableCore table2 =  fdb2.getTable("table1");
			assertNotNull(table2);
			
			// read back database contents		
			byte[] readBack;
			for(int x = 1; x < 15; x++)
			{
				readBack = table2.seekByteArray((long)x);
				int readInt = BuffRead.readInt(readBack, 10);
				assertEquals((2*x), readInt);
			}			
			
		// catch any stuff going wrong
		}
		catch (FemtoDBInvalidValueException e) {fail();} 
		catch (FemtoDBIOException e) {
			System.err.println(e);
			e.printStackTrace();
			fail();}
		catch (Exception e)
		{
			System.err.println(e);
			e.printStackTrace();
			fail();
		}
	}
	
	
	/*
	@Test
	public void testBackupOpenUsingPong()
	{	
		System.out.println("DatabaseTest1 - testBackupOpenUsingPong()");
		try{
			// ***************** CREATE DATABASE TO SHUTDOWN ******************
			FemtoDB fdb = new FemtoDB("debug1");
			fdb.setBackupDirectory("debug1backup");
			
			// make a fresh directory
			File f = new File("debug1");
			if(f.exists())FileUtils.recursiveDelete(f);
			f.mkdir();
			
			// this must follow the making of the directory!
			fdb.setPath("debug1");
			
			// ensure backup does not exist
			File fbackup = new File("debug1backup");
			if(fbackup.exists())FileUtils.recursiveDelete(fbackup);
			
			// create a table in the database
			TableCore table1 =  fdb.createTable("table1", "pk");
			
			// make the table operational
			table1.setRowsPerFile(5);
			table1.setRemoveOccupancyRatio(0.4);
			table1.setCombineOccupancyRatio(0.8);
			table1.addIntegerColumn("payload");
			table1.setCacheSize(140);
			table1.makeOperational();
				
			// insert a load of stuff
			byte[] toInsert = new byte[8+2+4];
			for(int x = 1; x < 15; x++)
			{
				BuffWrite.writeLong(toInsert, 0, (long)x);
				BuffWrite.writeShort(toInsert, 8, (10 * x));
				BuffWrite.writeInt(toInsert, 10, (2 * x));	
				table1.insertOrIgnoreByteArrayByPrimaryKey((long)x, toInsert);
			}
			
			// *****************  BACKUP TO PING ******************
			fdb.backup();

			// *****************  CHANGE THE DATA ******************
			for(int x = 1; x < 15; x++)
			{
				BuffWrite.writeLong(toInsert, 0, (long)x);
				BuffWrite.writeShort(toInsert, 8, (30 * x));
				BuffWrite.writeInt(toInsert, 10, (3 * x));	
				table1.insertOrIgnoreByteArrayByPrimaryKey((long)x, toInsert);
			}
			
			// *****************  BACKUP TO PONG ******************
			Thread.sleep(10);
			fdb.backup();
			
			// *****************  SHUTDOWN ******************
			fdb.shutdown();
				
			// *****************  OPEN THE SHUTDOWN DATABASE *********
			// make a fresh directory to restore database back
			File f2 = new File("debug1");
			if(f2.exists())FileUtils.recursiveDelete(f2);
			f2.mkdir();
			
			FemtoDB fdb2 = FemtoDB.open("debug1", "debug1backup");
			TableCore table2 =  fdb2.getTable("table1");
			assertNotNull(table2);
			
			// read back database contents... should match pong... most recent valid backup	
			byte[] readBack;
			for(int x = 1; x < 15; x++)
			{
				readBack = table2.seekByteArray((long)x);
				int readInt = BuffRead.readInt(readBack, 10);
				assertEquals((3*x), readInt);
			}			
			
		// catch any stuff going wrong
		}
		catch (FemtoDBInvalidValueException e) {fail();} 
		catch (FemtoDBIOException e) {
			System.err.println(e);
			e.printStackTrace();
			fail();}
		catch (Exception e)
		{
			System.err.println(e);
			e.printStackTrace();
			fail();
		}
	}
	
	*/

}
